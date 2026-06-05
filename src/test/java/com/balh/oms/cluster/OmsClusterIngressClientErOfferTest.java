package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import io.aeron.cluster.client.AeronCluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit coverage for the ER-offer daemon in {@link OmsClusterIngressClient#submitApplyExecutionReport}:
 * serialized offers, multi-offer per lock hold, and queue back-pressure. Avoids spinning up Aeron.
 */
class OmsClusterIngressClientErOfferTest {

    private static OmsConfig newConfig() {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory("/tmp/oms-er-offer-test-not-real");
        return cfg;
    }

    @Test
    void submitApplyExecutionReport_whenNotConnected_throwsIllegalState() {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());
        ApplyExecutionReportCommand cmd = sampleEr(client.nextCorrelationId());

        assertThatThrownBy(() -> client.submitApplyExecutionReport(cmd, Duration.ofMillis(100)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not connected");
    }

    @Test
    void erOfferDaemon_offersBurstUnderSingleLockAcquisition() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());
        AtomicInteger lockAcquisitions = new AtomicInteger();
        AtomicInteger offers = new AtomicInteger();
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            offers.incrementAndGet();
                            return 1L;
                        });

        setField(client, "client", cluster);
        setField(client, "closing", false);
        ReentrantLock lock = (ReentrantLock) declaredField(client, "clientLock").get(client);
        ReentrantLock countingLock =
                new ReentrantLock() {
                    @Override
                    public void lock() {
                        lockAcquisitions.incrementAndGet();
                        lock.lock();
                    }

                    @Override
                    public void lockInterruptibly() throws InterruptedException {
                        lockAcquisitions.incrementAndGet();
                        lock.lockInterruptibly();
                    }

                    @Override
                    public void unlock() {
                        lock.unlock();
                    }
                };
        setField(client, "clientLock", countingLock);
        invokeStartErOfferDaemonLocked(client);

        int n = 32;
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            CountDownLatch done = new CountDownLatch(n);
            for (int i = 0; i < n; i++) {
                long cid = client.nextCorrelationId();
                pool.submit(
                        () -> {
                            try {
                                client.submitApplyExecutionReport(sampleEr(cid), Duration.ofSeconds(5));
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                done.countDown();
                            }
                        });
            }
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
            client.close();
        }

        assertThat(offers.get()).isEqualTo(n);
        assertThat(lockAcquisitions.get())
                .as("daemon should amortise lock trips across a burst (not one acquisition per ER)")
                .isLessThan(n);
    }

    @Test
    void erOfferDaemon_concurrentSubmitsAllComplete() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(1L);

        setField(client, "client", cluster);
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);

        int n = 128;
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                long cid = client.nextCorrelationId();
                futures.add(
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        client.submitApplyExecutionReport(
                                                sampleEr(cid), Duration.ofSeconds(5));
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                                },
                                pool));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(15, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
            client.close();
        }

        assertThat(client.erOfferQueueDepthForTest()).isZero();
    }

    private static ApplyExecutionReportCommand sampleEr(long correlationId) {
        return new ApplyExecutionReportCommand(
                correlationId,
                UUID.randomUUID(),
                10_000_000_000L,
                650_000L,
                1L,
                0,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "venue-test",
                "exec-ref-" + correlationId,
                "",
                "{}");
    }

    private static Field declaredField(Object target, String name) throws NoSuchFieldException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        declaredField(target, name).set(target, value);
    }

    private static void invokeStartErOfferDaemonLocked(OmsClusterIngressClient client) throws Exception {
        Method m = OmsClusterIngressClient.class.getDeclaredMethod("startErOfferDaemonLocked");
        m.setAccessible(true);
        ReentrantLock lock = (ReentrantLock) declaredField(client, "clientLock").get(client);
        lock.lock();
        try {
            m.invoke(client);
        } finally {
            lock.unlock();
        }
    }
}
