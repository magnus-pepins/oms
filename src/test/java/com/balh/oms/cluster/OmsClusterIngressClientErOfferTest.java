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
        cfg.getCluster().getClient().getErOffer().setMaxPerLockPass(256);
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
    void submitApplyExecutionReportAsync_recordsCommitRoundTripWhenDaemonOffers() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), registry);
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(1L);
        setField(client, "client", cluster);
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);

        client.submitApplyExecutionReportAsync(sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5))
                .get(5, TimeUnit.SECONDS);

        assertThat(
                        registry.find(OmsClusterIngressClient.TIMER_NAME)
                                .tag(
                                        OmsClusterIngressClient.TAG_COMMAND,
                                        OmsClusterIngressClient.COMMAND_APPLY_EXECUTION_REPORT)
                                .tag(
                                        OmsClusterIngressClient.TAG_OUTCOME,
                                        OmsClusterIngressClient.Outcome.COMMIT.lowerName())
                                .timer())
                .isNotNull()
                .satisfies(t -> assertThat(t.count()).isEqualTo(1L));
        client.close();
    }

    @Test
    void submitApplyExecutionReportAsync_registersErOfferQueueDepthGauge() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), registry);
        assertThat(registry.find(OmsClusterIngressClient.GAUGE_ER_OFFER_QUEUE_DEPTH).gauge())
                .isNotNull()
                .satisfies(g -> assertThat(g.value()).isZero());
        client.close();
    }

    @Test
    void submitApplyExecutionReportAsync_returnsBeforeDaemonOffers() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());
        CountDownLatch offerStarted = new CountDownLatch(1);
        CountDownLatch releaseOffer = new CountDownLatch(1);
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            offerStarted.countDown();
                            assertThat(releaseOffer.await(5, TimeUnit.SECONDS)).isTrue();
                            return 1L;
                        });

        setField(client, "client", cluster);
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);

        CompletableFuture<Void> future =
                client.submitApplyExecutionReportAsync(sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
        assertThat(future.isDone()).isFalse();
        assertThat(offerStarted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(client.erOfferQueueDepthForTest()).isZero();

        releaseOffer.countDown();
        future.get(5, TimeUnit.SECONDS);
        client.close();
    }

    @Test
    void submitApplyExecutionReportAsync_whenQueueFull_failsFastWithoutBlocking() throws Exception {
        OmsConfig cfg = newConfig();
        int queueCap = cfg.getCluster().getClient().getErOffer().getQueueCapacity();
        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        setField(client, "client", Mockito.mock(AeronCluster.class));
        setField(client, "closing", false);

        for (int i = 0; i < queueCap; i++) {
            CompletableFuture<Void> enqueued =
                    client.submitApplyExecutionReportAsync(
                            sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
            assertThat(enqueued.isDone()).isFalse();
        }
        assertThat(client.erOfferQueueDepthForTest()).isEqualTo(queueCap);

        long blockedStart = System.nanoTime();
        CompletableFuture<Void> rejected =
                client.submitApplyExecutionReportAsync(
                        sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
        long blockedNanos = System.nanoTime() - blockedStart;

        assertThat(rejected.isCompletedExceptionally()).isTrue();
        assertThat(blockedNanos)
                .as("full queue should fail fast instead of parking the caller")
                .isLessThan(TimeUnit.MILLISECONDS.toNanos(50));
        assertThat(client.erOfferQueueDepthForTest()).isEqualTo(queueCap);
        client.close();
    }

    @Test
    void erOfferDaemon_greedyExtensionAmortisesLockAcrossQueueDrain() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().getErOffer().setMaxPerLockPass(64);
        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
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

        int n = 200;
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            CountDownLatch done = new CountDownLatch(n);
            for (int i = 0; i < n; i++) {
                long cid = client.nextCorrelationId();
                pool.submit(
                        () -> {
                            try {
                                client.submitApplyExecutionReportAsync(sampleEr(cid), Duration.ofSeconds(5))
                                        .get(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                done.countDown();
                            }
                        });
            }
            assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
            client.close();
        }

        assertThat(offers.get()).isEqualTo(n);
        assertThat(lockAcquisitions.get())
                .as("greedy extension should offer many frames per lock acquisition")
                .isLessThan(n / 4);
    }

    @Test
    void erOfferDaemon_unparkWakesIdleLoopWithoutPollTimeoutDelay() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().getErOffer().setDrainIntervalNanos(TimeUnit.MILLISECONDS.toNanos(50));
        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        CountDownLatch offerStarted = new CountDownLatch(1);
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            offerStarted.countDown();
                            return 1L;
                        });

        setField(client, "client", cluster);
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);

        // Let the daemon reach its idle park (would miss unpark if using poll(timeout)).
        Thread.sleep(20);

        long enqueueStart = System.nanoTime();
        CompletableFuture<Void> future =
                client.submitApplyExecutionReportAsync(sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
        assertThat(offerStarted.await(2, TimeUnit.SECONDS)).isTrue();
        future.get(2, TimeUnit.SECONDS);
        long wakeNanos = System.nanoTime() - enqueueStart;

        assertThat(wakeNanos)
                .as("unpark should wake daemon promptly, not after the 50ms drain poll timeout")
                .isLessThan(TimeUnit.MILLISECONDS.toNanos(15));
        client.close();
    }

    @Test
    void erOfferBatch_signalsDaemonOnceForManyAsyncEnqueues() throws Exception {
        OmsClusterIngressClient client = new OmsClusterIngressClient(newConfig(), new SimpleMeterRegistry());
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(1L);

        setField(client, "client", cluster);
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);
        client.resetErOfferSignalCountForTest();

        int n = 48;
        client.openErOfferBatch();
        try {
            for (int i = 0; i < n; i++) {
                CompletableFuture<Void> f =
                        client.submitApplyExecutionReportAsync(
                                sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
                assertThat(f.isDone()).isFalse();
            }
        } finally {
            client.closeErOfferBatch();
        }

        assertThat(client.erOfferSignalCountForTest())
                .as("batched enqueue should wake ER daemon once, not per frame")
                .isEqualTo(1);
        client.close();
    }

    @Test
    void erOfferOnly_interleavePollEveryNOfferAmortisesPollRounds() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        cfg.getCluster().getClient().getErOffer().setInterleavePollEveryOffers(8);
        cfg.getCluster().getClient().getErOffer().setInterleavePollCap(4);
        cfg.getCluster().getClient().getErOffer().setMaxPerLockPass(64);
        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        AtomicInteger pollRounds = new AtomicInteger();
        AtomicInteger offers = new AtomicInteger();
        AeronCluster cluster = Mockito.mock(AeronCluster.class);
        Mockito.when(cluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            offers.incrementAndGet();
                            return 1L;
                        });
        Mockito.when(cluster.pollEgress())
                .thenAnswer(
                        inv -> {
                            pollRounds.incrementAndGet();
                            return 0;
                        });

        setField(client, "client", cluster);
        setField(client, "closing", false);
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
                                client.submitApplyExecutionReportAsync(sampleEr(cid), Duration.ofSeconds(5))
                                        .get(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                done.countDown();
                            }
                        });
            }
            assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
            client.close();
        }

        assertThat(offers.get()).isEqualTo(n);
        int perOfferUpperBound = n * cfg.getCluster().getClient().getErOffer().getInterleavePollCap();
        assertThat(pollRounds.get())
                .as("poll egress should amortise across offers, not run after every offer")
                .isLessThan(perOfferUpperBound / 2);
        assertThat(pollRounds.get())
                .as("poll egress should still run during the burst")
                .isGreaterThan(0);
    }

    @Test
    void effectiveInterleavePollEveryOffers_lagAware_usesAggressiveIntervalWhenLagHighAndQueueShallow() {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        OmsConfig.Cluster.Client.ErOffer erOffer = cfg.getCluster().getClient().getErOffer();
        erOffer.setInterleavePollEveryOffers(16);
        erOffer.setInterleaveLagAwareEnabled(true);
        erOffer.setInterleaveLagBytesThreshold(4096L);
        erOffer.setInterleaveLagAwareQueueDepthThreshold(256);
        erOffer.setInterleaveLagAwarePollEveryOffers(1);

        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        client.setExcessEgressLagBytesSupplier(() -> 8192L);
        assertThat(client.effectiveInterleavePollEveryOffers()).isEqualTo(1);
        client.close();
    }

    @Test
    void effectiveInterleavePollEveryOffers_lagAware_fallsBackWhenQueueDeep() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        OmsConfig.Cluster.Client.ErOffer erOffer = cfg.getCluster().getClient().getErOffer();
        erOffer.setInterleavePollEveryOffers(16);
        erOffer.setInterleaveLagAwareEnabled(true);
        erOffer.setInterleaveLagBytesThreshold(4096L);
        erOffer.setInterleaveLagAwareQueueDepthThreshold(4);
        erOffer.setInterleaveLagAwarePollEveryOffers(1);
        erOffer.setQueueCapacity(8);

        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        setField(client, "client", Mockito.mock(AeronCluster.class));
        setField(client, "closing", false);
        client.setExcessEgressLagBytesSupplier(() -> 8192L);
        for (int i = 0; i < 4; i++) {
            CompletableFuture<Void> enqueued =
                    client.submitApplyExecutionReportAsync(
                            sampleEr(client.nextCorrelationId()), Duration.ofSeconds(5));
            assertThat(enqueued.isDone()).isFalse();
        }
        assertThat(client.erOfferQueueDepthForTest()).isEqualTo(4);
        assertThat(client.effectiveInterleavePollEveryOffers()).isEqualTo(16);
        client.close();
    }

    @Test
    void erOfferOnly_lagAwareInterleave_pollsMoreAggressivelyWhenLagHighAndQueueShallow() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        OmsConfig.Cluster.Client.ErOffer erOffer = cfg.getCluster().getClient().getErOffer();
        erOffer.setInterleavePollEveryOffers(16);
        erOffer.setInterleaveLagAwareEnabled(true);
        erOffer.setInterleaveLagBytesThreshold(4096L);
        erOffer.setInterleaveLagAwareQueueDepthThreshold(256);
        erOffer.setInterleaveLagAwarePollEveryOffers(1);
        erOffer.setInterleavePollCap(4);
        erOffer.setMaxPerLockPass(64);

        OmsClusterIngressClient lagAwareClient = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        lagAwareClient.setExcessEgressLagBytesSupplier(() -> 8192L);
        AtomicInteger lagAwarePollRounds = new AtomicInteger();
        AtomicInteger lagAwareOffers = new AtomicInteger();
        AeronCluster lagAwareCluster = Mockito.mock(AeronCluster.class);
        Mockito.when(lagAwareCluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            lagAwareOffers.incrementAndGet();
                            return 1L;
                        });
        Mockito.when(lagAwareCluster.pollEgress())
                .thenAnswer(
                        inv -> {
                            lagAwarePollRounds.incrementAndGet();
                            return 0;
                        });

        setField(lagAwareClient, "client", lagAwareCluster);
        setField(lagAwareClient, "closing", false);
        invokeStartErOfferDaemonLocked(lagAwareClient);

        OmsConfig baselineCfg = newConfig();
        baselineCfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        baselineCfg.getCluster().getClient().getErOffer().setInterleavePollEveryOffers(16);
        baselineCfg.getCluster().getClient().getErOffer().setInterleaveLagAwareEnabled(false);
        baselineCfg.getCluster().getClient().getErOffer().setInterleavePollCap(4);
        baselineCfg.getCluster().getClient().getErOffer().setMaxPerLockPass(64);
        OmsClusterIngressClient baselineClient = new OmsClusterIngressClient(baselineCfg, new SimpleMeterRegistry());
        AtomicInteger baselinePollRounds = new AtomicInteger();
        AtomicInteger baselineOffers = new AtomicInteger();
        AeronCluster baselineCluster = Mockito.mock(AeronCluster.class);
        Mockito.when(baselineCluster.offer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt()))
                .thenAnswer(
                        inv -> {
                            baselineOffers.incrementAndGet();
                            return 1L;
                        });
        Mockito.when(baselineCluster.pollEgress())
                .thenAnswer(
                        inv -> {
                            baselinePollRounds.incrementAndGet();
                            return 0;
                        });

        setField(baselineClient, "client", baselineCluster);
        setField(baselineClient, "closing", false);
        invokeStartErOfferDaemonLocked(baselineClient);

        int n = 32;
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            CountDownLatch done = new CountDownLatch(n * 2);
            for (int i = 0; i < n; i++) {
                long cid = lagAwareClient.nextCorrelationId();
                pool.submit(
                        () -> {
                            try {
                                lagAwareClient.submitApplyExecutionReportAsync(sampleEr(cid), Duration.ofSeconds(5))
                                        .get(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                done.countDown();
                            }
                        });
                long baselineCid = baselineClient.nextCorrelationId();
                pool.submit(
                        () -> {
                            try {
                                baselineClient.submitApplyExecutionReportAsync(
                                                sampleEr(baselineCid), Duration.ofSeconds(5))
                                        .get(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            } finally {
                                done.countDown();
                            }
                        });
            }
            assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
            lagAwareClient.close();
            baselineClient.close();
        }

        assertThat(lagAwareOffers.get()).isEqualTo(n);
        assertThat(baselineOffers.get()).isEqualTo(n);
        assertThat(lagAwarePollRounds.get())
                .as("lag-aware interleave should poll egress more often than baseline every-16")
                .isGreaterThan(baselinePollRounds.get());
    }

    @Test
    void erOfferOnlyRole_singleErDaemonWithoutCompetingPollerThreads() throws Exception {
        OmsConfig cfg = newConfig();
        cfg.getCluster().getClient().setRole(ClusterClientRole.ER_OFFER_ONLY);
        OmsClusterIngressClient client = new OmsClusterIngressClient(cfg, new SimpleMeterRegistry());
        setField(client, "client", Mockito.mock(AeronCluster.class));
        setField(client, "closing", false);
        invokeStartErOfferDaemonLocked(client);

        assertThat(declaredField(client, "egressPollerThread").get(client)).isNull();
        assertThat(declaredField(client, "erOfferDaemonThread").get(client)).isNotNull();
        client.close();
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
        invokePrivateLocked(client, "startErOfferDaemonLocked");
    }

    private static void invokePrivateLocked(OmsClusterIngressClient client, String methodName) throws Exception {
        Method m = OmsClusterIngressClient.class.getDeclaredMethod(methodName);
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
