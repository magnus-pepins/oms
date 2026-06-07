package com.balh.oms.venueegress;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EgressCompletionTrackerTest {

    @Test
    void inOrderCompletion_advancesEachStep() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.register(20L);
        t.register(30L);

        t.complete(10L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(10L, 1));
        t.complete(20L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(20L, 1));
        t.complete(30L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(30L, 1));
        assertThat(t.isDrained()).isTrue();
    }

    @Test
    void outOfOrderCompletion_neverAdvancesPastAGap() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.register(20L);
        t.register(30L);

        // 20 and 30 finish before 10 — cursor must NOT advance: a crash here must replay 10..30.
        t.complete(20L);
        t.complete(30L);
        assertThat(t.pollContiguous()).isEmpty();
        assertThat(t.inFlight()).isEqualTo(3);

        // 10 finally completes — the whole contiguous run flushes at once, to the highest (30).
        t.complete(10L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(30L, 3));
        assertThat(t.isDrained()).isTrue();
    }

    @Test
    void partialContiguousPrefix_advancesOnlyToTheGap() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.register(20L);
        t.register(30L);
        t.register(40L);

        t.complete(10L);
        t.complete(20L);
        t.complete(40L); // 30 still in flight
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(20L, 2));
        assertThat(t.inFlight()).isEqualTo(2); // 30, 40 remain

        t.complete(30L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(40L, 2));
        assertThat(t.isDrained()).isTrue();
    }

    @Test
    void pollWithNothingComplete_isEmpty() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(5L);
        assertThat(t.pollContiguous()).isEmpty();
        assertThat(t.isDrained()).isFalse();
    }

    @Test
    void completionForUnregisteredPosition_isIgnored() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.complete(99L); // never registered
        assertThat(t.pollContiguous()).isEmpty();
        t.complete(10L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(10L, 1));
    }

    @Test
    void register_mustBeStrictlyIncreasing() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        assertThatThrownBy(() -> t.register(10L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> t.register(5L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void freshTracker_isDrained() {
        assertThat(new EgressCompletionTracker().isDrained()).isTrue();
        assertThat(new EgressCompletionTracker().pollContiguous()).isEmpty();
    }

    @Test
    void cursorOnlyCheckpoint_waitsForPriorAdmits_notLaterOnes() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.register(20L);
        t.registerCursorOnly(25L);
        t.register(30L);

        t.complete(10L);
        t.complete(20L);
        // One poll flushes the contiguous admits and the unblocked cursor-only checkpoint.
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(25L, 3));
        assertThat(t.inFlight()).isEqualTo(1);
        assertThat(t.isDrained()).isFalse();

        t.complete(30L);
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(30L, 1));
        assertThat(t.isDrained()).isTrue();
    }

    @Test
    void cursorOnlyCheckpoint_interleavedWithAdmits_flushesInLogOrder() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.register(20L);
        t.registerCursorOnly(25L);
        t.register(30L);

        t.complete(10L);
        t.complete(20L);
        t.complete(30L);

        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(30L, 4));
        assertThat(t.isDrained()).isTrue();
    }

    @Test
    void registerCursorOnly_mustBeStrictlyIncreasing() {
        EgressCompletionTracker t = new EgressCompletionTracker();
        t.register(10L);
        t.registerCursorOnly(20L);
        assertThatThrownBy(() -> t.registerCursorOnly(20L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> t.register(15L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentComplete_doesNotLoseCompletions() throws Exception {
        EgressCompletionTracker t = new EgressCompletionTracker();
        int n = 256;
        for (long i = 1; i <= n; i++) {
            t.register(i * 10L);
        }
        int threads = 32;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(n);
            for (long i = 1; i <= n; i++) {
                long position = i * 10L;
                pool.submit(
                        () -> {
                            try {
                                start.await();
                                t.complete(position);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }
            start.countDown();
            assertThat(done.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(t.pollContiguous()).contains(new EgressCompletionTracker.ContiguousDrain(n * 10L, n));
        assertThat(t.isDrained()).isTrue();
    }
}
