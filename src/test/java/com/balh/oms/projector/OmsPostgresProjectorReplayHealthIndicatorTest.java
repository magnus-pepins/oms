package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.balh.oms.config.OmsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Status;

@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorReplayHealthIndicatorTest {

    @Mock private OmsPostgresProjector projector;

    private OmsConfig config;
    private OmsPostgresProjectorReplayHealthIndicator indicator;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getCluster().getProjector().setReadinessMaxReplayStallMs(30_000L);
        indicator = new OmsPostgresProjectorReplayHealthIndicator(projector, config);
    }

    @Test
    void downWhenReplayThreadNotAlive() {
        when(projector.isReplayLoopAlive()).thenReturn(false);
        when(projector.isReplayLoopRunning()).thenReturn(true);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void upAtIdleLiveTail() {
        when(projector.isReplayLoopAlive()).thenReturn(true);
        when(projector.millisSinceLastCursorAdvance()).thenReturn(120_000L);
        when(projector.replayPositionHasBacklog()).thenReturn(false);
        when(projector.applyQueueHasBacklog()).thenReturn(false);
        when(projector.lastAppliedPosition()).thenReturn(4576L);
        when(projector.liveReplayUpperBound()).thenReturn(4576L);
        when(projector.replayBacklogFragments()).thenReturn(0L);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void downWhenBacklogStalledBeyondThreshold() {
        when(projector.isReplayLoopAlive()).thenReturn(true);
        when(projector.millisSinceLastCursorAdvance()).thenReturn(31_000L);
        when(projector.replayPositionHasBacklog()).thenReturn(true);
        when(projector.applyQueueHasBacklog()).thenReturn(false);
        when(projector.lastAppliedPosition()).thenReturn(3904L);
        when(projector.liveReplayUpperBound()).thenReturn(4576L);
        when(projector.replayBacklogFragments()).thenReturn(672L);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }
}
