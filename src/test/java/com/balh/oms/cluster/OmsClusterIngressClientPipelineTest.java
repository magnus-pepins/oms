package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Slice 4n unit-coverage for the pipelined {@link OmsClusterIngressClient}: exercises the
 * future-keyed demux ({@link OmsClusterIngressClient#completeWaiter}), the orphan-reply path,
 * close()-time drain of in-flight submitters, and the negative path of
 * {@link OmsClusterIngressClient#submitAcceptOrder} when the underlying cluster client is not
 * connected. End-to-end happy-path correctness is still covered by
 * {@link OmsClusterIngressClientIT} against a real single-node cluster.
 *
 * <p>These tests deliberately avoid spinning up an Aeron media driver — they probe the
 * pipelining bookkeeping (the {@code pending} map, future completion, exception unwind) which
 * is what slice 4n actually changed.
 */
class OmsClusterIngressClientPipelineTest {

    private static OmsConfig newConfig() {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory("/tmp/oms-pipeline-test-not-real");
        return cfg;
    }

    @Test
    void submitAcceptOrder_whenNotConnected_unwindsPendingAndThrowsIllegalState() {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());

        AcceptOrderCommand cmd = newAcceptOrderCommand(client.nextCorrelationId());

        assertThatThrownBy(() -> client.submitAcceptOrder(cmd, Duration.ofMillis(100)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not connected");

        assertThat(client.pendingCountForTest())
                .as("pending must be drained when offer fails synchronously — otherwise we leak"
                        + " futures across submits and corner cases like duplicate correlationId"
                        + " checks become wrong on the next call")
                .isZero();
    }

    @Test
    void completeWaiter_routesAcceptedReplyToWaitingFuture() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());

        long correlationId = client.nextCorrelationId();
        CompletableFuture<AdmissionResult> waiter = injectPendingWaiter(client, correlationId);

        AdmissionResult.Accepted reply = new AdmissionResult.Accepted(
                new OrderAcceptedEvent(correlationId, UUID.randomUUID(), 1, false, 0L));
        client.completeWaiter(correlationId, reply);

        assertThat(waiter).isCompleted();
        assertThat(waiter.get(0, TimeUnit.SECONDS)).isSameAs(reply);
        assertThat(client.pendingCountForTest()).isZero();
    }

    @Test
    void completeWaiter_orphanReplyDoesNotThrow() {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());

        // Caller already gave up (or never registered); a late egress reply must be a debug log,
        // not a NPE or runtime crash that would topple the egress poller thread.
        AdmissionResult.Accepted reply = new AdmissionResult.Accepted(
                new OrderAcceptedEvent(424242L, UUID.randomUUID(), 1, false, 0L));

        client.completeWaiter(424242L, reply);

        assertThat(client.pendingCountForTest()).isZero();
    }

    @Test
    void close_drainsPendingFuturesExceptionally() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());

        long c1 = client.nextCorrelationId();
        long c2 = client.nextCorrelationId();
        CompletableFuture<AdmissionResult> w1 = injectPendingWaiter(client, c1);
        CompletableFuture<AdmissionResult> w2 = injectPendingWaiter(client, c2);

        client.close();

        assertThat(w1).isCompletedExceptionally();
        assertThat(w2).isCompletedExceptionally();
        assertThatThrownBy(() -> w1.get(0, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(client.pendingCountForTest()).isZero();
    }

    @Test
    void completeWaiter_concurrentRepliesEachLandOnTheirOwnFuture() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());

        int n = 256;
        CompletableFuture<AdmissionResult>[] waiters = newWaiterArray(n);
        long[] ids = new long[n];
        for (int i = 0; i < n; i++) {
            ids[i] = client.nextCorrelationId();
            waiters[i] = injectPendingWaiter(client, ids[i]);
        }

        // Hammer the demux from many threads simultaneously to flush out any
        // ConcurrentModificationException / lost-update / double-complete bugs.
        var pool = Executors.newFixedThreadPool(16);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            AtomicInteger failures = new AtomicInteger(0);
            for (int i = 0; i < n; i++) {
                final long id = ids[i];
                pool.submit(() -> {
                    try {
                        start.await();
                        client.completeWaiter(id,
                                new AdmissionResult.Accepted(
                                        new OrderAcceptedEvent(id, UUID.randomUUID(), 1, false, 0L)));
                    } catch (Throwable t) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failures.get()).isZero();
        } finally {
            pool.shutdownNow();
        }

        for (int i = 0; i < n; i++) {
            assertThat(waiters[i]).as("waiter %d", i).isCompleted();
            AdmissionResult.Accepted r = (AdmissionResult.Accepted) waiters[i].get(0, TimeUnit.SECONDS);
            assertThat(r.event().correlationId()).isEqualTo(ids[i]);
        }
        assertThat(client.pendingCountForTest()).isZero();
    }

    private static AcceptOrderCommand newAcceptOrderCommand(long correlationId) {
        return new AcceptOrderCommand(
                correlationId,
                UUID.randomUUID(),
                System.nanoTime(),
                1_000_000_000L,
                0L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                UUID.randomUUID().toString(),
                "idempotency-" + correlationId,
                "hash-" + correlationId,
                "AAPL",
                null);
    }

    /**
     * Reaches into {@link OmsClusterIngressClient}'s {@code pending} map so the test can drive
     * the demux without forcing a real {@code submitAcceptOrder} call (which would require a
     * connected cluster). This is the smallest reflection footprint that lets us cover
     * {@code completeWaiter} + {@code close()} drain in pure unit form.
     */
    @SuppressWarnings("unchecked")
    private static CompletableFuture<AdmissionResult> injectPendingWaiter(
            OmsClusterIngressClient client, long correlationId) throws Exception {
        java.lang.reflect.Field field = OmsClusterIngressClient.class.getDeclaredField("pending");
        field.setAccessible(true);
        java.util.concurrent.ConcurrentHashMap<Long, CompletableFuture<AdmissionResult>> map =
                (java.util.concurrent.ConcurrentHashMap<Long, CompletableFuture<AdmissionResult>>)
                        field.get(client);
        CompletableFuture<AdmissionResult> waiter = new CompletableFuture<>();
        if (map.putIfAbsent(correlationId, waiter) != null) {
            throw new IllegalStateException("test seeded duplicate correlationId=" + correlationId);
        }
        return waiter;
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<AdmissionResult>[] newWaiterArray(int n) {
        return (CompletableFuture<AdmissionResult>[]) new CompletableFuture<?>[n];
    }
}
