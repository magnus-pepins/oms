package com.balh.oms.venueegress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.venue.VenueRouteOrderClient;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Replay-loop drain tuning: burst {@code replay.poll}, idle park nanos, fragment limit, and replay
 * thread priority. Does not stand up Aeron Archive — exercises package-private poll helpers only.
 */
@ExtendWith(MockitoExtension.class)
class OmsVenueEgressReplayPollLoopTest {

    private static final int FRAGMENT_LIMIT = 64;

    @Mock OmsVenueEgressCursorRepository cursorRepository;
    @Mock VenueRouteOrderClient routeClient;
    @Mock OmsClusterIngressClient clusterIngressClient;

    OmsVenueEgressService service;

    @BeforeEach
    void setUp() {
        service =
                new OmsVenueEgressService(
                        new OmsConfig(),
                        cursorRepository,
                        new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                        new com.fasterxml.jackson.databind.ObjectMapper(),
                        null,
                        null);
        service.markRunningForTesting();
        service.setReplayPollConfigForTesting(FRAGMENT_LIMIT, 10_000L);
    }

    @Test
    void venueEgressConfig_defaults_targetFourHundredFragmentsPerSecond() {
        OmsConfig.Cluster.VenueEgress cfg = new OmsConfig().getCluster().getVenueEgress();

        assertThat(cfg.getFragmentLimit()).isGreaterThanOrEqualTo(512);
        assertThat(cfg.getPollParkNanos()).isLessThanOrEqualTo(10_000L);
        assertThat(cfg.getReplayThreadPriority()).isEqualTo(Thread.MAX_PRIORITY);

        // Theoretical idle-tail ceiling: 1/pollParkNanos polls/s * fragmentLimit frags/poll.
        double maxIdleTailFragsPerSec =
                (1_000_000_000.0 / cfg.getPollParkNanos()) * cfg.getFragmentLimit();
        assertThat(maxIdleTailFragsPerSec).isGreaterThanOrEqualTo(400.0);
    }

    @Test
    void effectiveReplayFragmentLimit_floorsAt4096ForHighAdmitDrain() {
        assertThat(OmsVenueEgressService.effectiveReplayFragmentLimit(512)).isEqualTo(4096);
        assertThat(OmsVenueEgressService.effectiveReplayFragmentLimit(2048)).isEqualTo(4096);
        assertThat(OmsVenueEgressService.effectiveReplayFragmentLimit(8192)).isEqualTo(8192);
    }

    @Test
    void venueEgressConfig_replayIdleTailPolls_defaultsToSixteen() {
        OmsConfig.Cluster.VenueEgress cfg = new OmsConfig().getCluster().getVenueEgress();
        assertThat(cfg.getReplayIdleTailPolls()).isEqualTo(16);
        cfg.setReplayIdleTailPolls(0);
        assertThat(cfg.getReplayIdleTailPolls()).isEqualTo(1);
    }

    @Test
    void pollReplayIdleTail_retriesConfiguredSpinCountBeforeGivingUp() {
        Subscription replay = mock(Subscription.class);
        FragmentHandler handler = (buffer, offset, length, header) -> {};
        when(replay.poll(any(), eq(FRAGMENT_LIMIT))).thenReturn(0);
        service.setReplayPollConfigForTesting(FRAGMENT_LIMIT, 10_000L, 5);

        assertThat(service.pollReplayIdleTail(replay, handler)).isZero();
        verify(replay, times(5)).poll(any(), eq(FRAGMENT_LIMIT));
    }

    @Test
    void replayThreadPriority_clampedToThreadRange() {
        OmsConfig.Cluster.VenueEgress cfg = new OmsConfig().getCluster().getVenueEgress();

        cfg.setReplayThreadPriority(Thread.MIN_PRIORITY - 5);
        assertThat(cfg.getReplayThreadPriority()).isEqualTo(Thread.MIN_PRIORITY);

        cfg.setReplayThreadPriority(Thread.MAX_PRIORITY + 5);
        assertThat(cfg.getReplayThreadPriority()).isEqualTo(Thread.MAX_PRIORITY);
    }

    @Test
    void applyReplayThreadPriority_setsThreadPriority() {
        Thread t = new Thread(() -> {});
        OmsVenueEgressService.applyReplayThreadPriority(t, Thread.MAX_PRIORITY);
        assertThat(t.getPriority()).isEqualTo(Thread.MAX_PRIORITY);
    }

    @Test
    void pollReplayBurst_tightSpinsUntilAeronReturnsZero() {
        Subscription replay = mock(Subscription.class);
        FragmentHandler handler = (buffer, offset, length, header) -> {};
        AtomicInteger calls = new AtomicInteger();
        when(replay.poll(any(), eq(FRAGMENT_LIMIT)))
                .thenAnswer(
                        inv -> {
                            int n = calls.getAndIncrement();
                            return switch (n) {
                                case 0 -> FRAGMENT_LIMIT;
                                case 1 -> 17;
                                default -> 0;
                            };
                        });

        int total = service.pollReplayBurst(replay, handler);

        assertThat(total).isEqualTo(FRAGMENT_LIMIT + 17);
        verify(replay, times(3)).poll(any(), eq(FRAGMENT_LIMIT));
    }

    @Test
    void pollReplayBurst_returnsZeroWhenSubscriptionIdle() {
        Subscription replay = mock(Subscription.class);
        FragmentHandler handler = (buffer, offset, length, header) -> {};
        when(replay.poll(any(), eq(FRAGMENT_LIMIT))).thenReturn(0);

        assertThat(service.pollReplayBurst(replay, handler)).isZero();
        verify(replay, times(1)).poll(any(), eq(FRAGMENT_LIMIT));
    }

    @Test
    void parkReplayIdleAfterPoll_whenPipelineBacklog_doesNotParkConfiguredSlice() {
        when(routeClient.routeAdmittedOrderAsync(any()))
                .thenReturn(new CompletableFuture<>());
        OmsVenueEgressService pipelined =
                new OmsVenueEgressService(
                        new OmsConfig(),
                        cursorRepository,
                        new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                        new com.fasterxml.jackson.databind.ObjectMapper(),
                        routeClient,
                        clusterIngressClient);
        pipelined.markRunningForTesting();
        pipelined.enablePipelineForTesting(4, Runnable::run, Runnable::run);
        pipelined.pipelineDispatchAdmitForTesting(
                new OrderAdmittedEvent(
                        UUID.randomUUID(),
                        1L,
                        1L,
                        10_000_000_000L,
                        0L,
                        0,
                        0,
                        (byte) 0,
                        (byte) 0,
                        (byte) 2,
                        "acct",
                        "intent",
                        "hash",
                        "PREDMKT-TEST",
                        null,
                        null),
                128L);
        assertThat(pipelined.pipelineInFlightForTesting()).isPositive();

        long before = System.nanoTime();
        pipelined.parkReplayIdleAfterPoll(pipelined.pipelineForTesting());
        long elapsedNanos = System.nanoTime() - before;

        assertThat(elapsedNanos).isLessThan(5_000_000L);
    }

    @Test
    void parkReplayIdleAfterPoll_whenPipelineDrained_usesConfiguredPark() {
        service.enablePipelineForTesting(4, Runnable::run, Runnable::run);
        assertThat(service.pipelineInFlightForTesting()).isZero();

        long before = System.nanoTime();
        service.parkReplayIdleAfterPoll(service.pipelineForTesting());
        long elapsedNanos = System.nanoTime() - before;

        assertThat(elapsedNanos).isGreaterThanOrEqualTo(5_000L);
    }

    @Test
    void replayPollHasLagAdaptiveDrain_triggersOnExcessByteLagOrPendingRoutes() {
        assertThat(
                        OmsVenueEgressService.replayPollHasLagAdaptiveDrain(
                                4096L, 4096L, 0, 256))
                .isTrue();
        assertThat(
                        OmsVenueEgressService.replayPollHasLagAdaptiveDrain(
                                100L, 4096L, 256, 256))
                .isTrue();
        assertThat(
                        OmsVenueEgressService.replayPollHasLagAdaptiveDrain(
                                100L, 4096L, 10, 256))
                .isFalse();
    }

    @Test
    void computeExcessReplayLagBytes_subtractsPipelinedFloor() {
        assertThat(OmsVenueEgressService.computeExcessReplayLagBytes(10_000L, 8_000L, 512L))
                .isEqualTo(1_488L);
        assertThat(OmsVenueEgressService.computeExcessReplayLagBytes(10_000L, 9_800L, 512L))
                .isZero();
    }

    @Test
    void effectiveReplayFragmentLimit_raisesCapWhenLagAdaptiveDrainActive() {
        assertThat(
                        OmsVenueEgressService.effectiveReplayFragmentLimit(
                                4096, true, 8192))
                .isEqualTo(8192);
        assertThat(
                        OmsVenueEgressService.effectiveReplayFragmentLimit(
                                4096, false, 8192))
                .isEqualTo(4096);
        assertThat(
                        OmsVenueEgressService.effectiveReplayFragmentLimit(
                                512, false, 8192))
                .isEqualTo(4096);
    }

    @Test
    void effectiveReplayIdleTailPolls_extendsSpinBudgetWhenLagAdaptiveDrainActive() {
        assertThat(OmsVenueEgressService.effectiveReplayIdleTailPolls(16, true, 64)).isEqualTo(64);
        assertThat(OmsVenueEgressService.effectiveReplayIdleTailPolls(16, false, 64)).isEqualTo(16);
    }

    @Test
    void parkReplayIdleAfterPoll_whenLagAdaptiveByteLag_doesNotParkConfiguredSlice() {
        service.setReplayLagAdaptiveConfigForTesting(1024L, 999, 8192, 64, 0L);
        service.setReplayRecordingUpperBoundForTesting(20_000L);
        service.setLastAppliedPositionForTesting(10_000L);

        long before = System.nanoTime();
        service.parkReplayIdleAfterPoll(null);
        long elapsedNanos = System.nanoTime() - before;

        assertThat(elapsedNanos).isLessThan(5_000_000L);
    }

    @Test
    void pollReplayBurst_usesLagAdaptiveFragmentLimitWhenByteLagHigh() {
        service.setReplayPollConfigForTesting(4096, 10_000L);
        service.setReplayLagAdaptiveConfigForTesting(1024L, 999, 8192, 64, 0L);
        service.setReplayRecordingUpperBoundForTesting(20_000L);
        service.setLastAppliedPositionForTesting(10_000L);

        Subscription replay = mock(Subscription.class);
        FragmentHandler handler = (buffer, offset, length, header) -> {};
        when(replay.poll(any(), eq(8192))).thenReturn(0);

        service.pollReplayBurst(replay, handler);

        verify(replay).poll(any(), eq(8192));
    }

    @Test
    void venueEgressConfig_replayLagAdaptiveDefaults() {
        OmsConfig.Cluster.VenueEgress cfg = new OmsConfig().getCluster().getVenueEgress();

        assertThat(cfg.getReplayLagAdaptiveExcessLagBytesThreshold()).isEqualTo(4096L);
        assertThat(cfg.getReplayLagAdaptivePendingRoutesThreshold()).isEqualTo(256);
        assertThat(cfg.getReplayLagAdaptiveFragmentLimit()).isEqualTo(8192);
        assertThat(cfg.getReplayLagAdaptiveIdleTailPolls()).isEqualTo(64);
    }
}
