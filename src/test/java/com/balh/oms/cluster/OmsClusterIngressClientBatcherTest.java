package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 4 Tier 2.5 phase D-6 unit-coverage for the admit-batcher path on
 * {@link OmsClusterIngressClient}: enqueue / dequeue mechanics, queue-full back-pressure,
 * close()-time drain, and the demux-future contract being identical to the unbatched path.
 *
 * <p>End-to-end happy-path correctness against a real Aeron cluster lives in the integration
 * test suite ({@code OrderIngressClusterIntegrationTest}). These tests deliberately avoid
 * starting a media driver — they probe the batcher bookkeeping which is what D-6 actually
 * changed.
 */
class OmsClusterIngressClientBatcherTest {

    private static OmsConfig newBatchConfig(int queueCapacity, int maxBatchSize) {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory("/tmp/oms-batcher-test-not-real");
        OmsConfig.Cluster.Client.AdmitBatch ab = cfg.getCluster().getClient().getAdmitBatch();
        ab.setEnabled(true);
        ab.setQueueCapacity(queueCapacity);
        ab.setMaxBatchSize(maxBatchSize);
        ab.setEnqueueParkNanos(100L);
        ab.setFlushIntervalNanos(1_000L);
        return cfg;
    }

    @Test
    void submitAcceptOrder_withBatcherEnabled_enqueuesAndRegistersPending() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(
                newBatchConfig(64, 8), new SimpleMeterRegistry());

        long correlationId = client.nextCorrelationId();
        AcceptOrderCommand cmd = newCmd(correlationId);

        // submitAcceptOrder will register in pending + enqueue, then park on the future.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> error = new AtomicReference<>();
        try {
            pool.submit(() -> {
                try {
                    client.submitAcceptOrder(cmd, Duration.ofMillis(500));
                } catch (Throwable t) {
                    error.set(t);
                }
            });

            // Wait until the queue + pending map see the entry (poll, don't sleep arbitrarily).
            BlockingQueue<?> queue = admitBatchQueue(client);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline && (queue.isEmpty() || client.pendingCountForTest() == 0)) {
                Thread.onSpinWait();
            }

            assertThat(client.pendingCountForTest()).isEqualTo(1);
            assertThat(queue).hasSize(1);

            // Drain the queue and complete the future via the egress-demux contract.
            Object pending = queue.poll();
            assertThat(pending).isNotNull();
            client.completeWaiter(correlationId, new AdmissionResult.Accepted(
                    new OrderAcceptedEvent(correlationId, UUID.randomUUID(), 1, false, 0L)));

            // The submit thread now unparks; verify it returned without error.
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < until && error.get() == null && client.pendingCountForTest() != 0) {
                Thread.onSpinWait();
            }
            assertThat(error.get()).isNull();
            assertThat(client.pendingCountForTest()).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void submitAcceptOrder_queueFullPastDeadline_throwsTimeoutAndUnwindsPending() throws Exception {
        // OmsConfig clamps queueCapacity to >= 64. Use the floor and pre-fill it so the next
        // enqueue is guaranteed to observe a full queue.
        int capacity = 64;
        OmsClusterIngressClient client = new OmsClusterIngressClient(
                newBatchConfig(capacity, 8), new SimpleMeterRegistry());

        // Pre-fill the queue via reflection. .connect() was not called, so no daemon runs and
        // these placeholders stay parked for the lifetime of the test.
        @SuppressWarnings({"rawtypes", "unchecked"})
        BlockingQueue raw = admitBatchQueue(client);
        for (int i = 0; i < capacity; i++) {
            assertThat(raw.offer(new Object())).isTrue();
        }
        assertThat(raw).hasSize(capacity);

        AcceptOrderCommand cmd = newCmd(client.nextCorrelationId());
        long t0 = System.nanoTime();
        assertThatThrownBy(() -> client.submitAcceptOrder(cmd, Duration.ofMillis(50)))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("admit-batch queue full");
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertThat(elapsedMs)
                .as("submit must park-and-retry until its own deadline, not return synchronously")
                .isGreaterThanOrEqualTo(40L);
        assertThat(client.pendingCountForTest())
                .as("on TimeoutException the pending entry must be unwound")
                .isZero();
    }

    @Test
    void close_drainsBatcherQueueAndCompletesFuturesExceptionally() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(
                newBatchConfig(64, 8), new SimpleMeterRegistry());

        long correlationId = client.nextCorrelationId();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch enqueued = new CountDownLatch(1);
        try {
            pool.submit(() -> {
                try {
                    AcceptOrderCommand cmd = newCmd(correlationId);
                    enqueued.countDown();
                    client.submitAcceptOrder(cmd, Duration.ofSeconds(5));
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            assertThat(enqueued.await(2, TimeUnit.SECONDS)).isTrue();

            // Wait until the entry actually lands in the queue + pending map.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline && client.pendingCountForTest() == 0) {
                Thread.onSpinWait();
            }
            assertThat(client.pendingCountForTest()).isEqualTo(1);

            client.close();

            // The submit thread must unpark with an exception (close drained pending +
            // batcher queue while no daemon was running to consume it on the success path).
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < until && error.get() == null) {
                Thread.onSpinWait();
            }
            assertThat(error.get())
                    .isNotNull()
                    .satisfiesAnyOf(
                            t -> assertThat(t).isInstanceOf(ExecutionException.class)
                                    .hasCauseInstanceOf(IllegalStateException.class),
                            t -> assertThat(t).isInstanceOf(IllegalStateException.class));
            assertThat(client.pendingCountForTest()).isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void admitBatchDisabled_keepsUnbatchedPath_doesNotAllocateQueue() {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory("/tmp/oms-batcher-test-not-real");
        // admit-batch left at default (disabled).

        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());

        // The unbatched path remains: not-connected throws IllegalStateException, queue stays
        // null so we cannot reflect it (it's the same path slice 4n covered).
        AcceptOrderCommand cmd = newCmd(client.nextCorrelationId());
        assertThatThrownBy(() -> client.submitAcceptOrder(cmd, Duration.ofMillis(50)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not connected");
        assertThat(client.pendingCountForTest()).isZero();
    }

    private static AcceptOrderCommand newCmd(long correlationId) {
        return new AcceptOrderCommand(
                correlationId,
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* quantityScaled = */ 1_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-" + correlationId,
                "idem-" + correlationId,
                "hash-" + correlationId,
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }

    @SuppressWarnings("unchecked")
    private static <E> BlockingQueue<E> admitBatchQueue(OmsClusterIngressClient client) throws Exception {
        Field f = OmsClusterIngressClient.class.getDeclaredField("admitBatchQueue");
        f.setAccessible(true);
        BlockingQueue<E> q = (BlockingQueue<E>) f.get(client);
        if (q == null) {
            throw new IllegalStateException(
                    "admitBatchQueue is null — admit-batch must be enabled on the test config");
        }
        return q;
    }
}
