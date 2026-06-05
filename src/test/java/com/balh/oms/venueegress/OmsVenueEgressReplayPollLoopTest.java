package com.balh.oms.venueegress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.config.OmsConfig;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
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
}
