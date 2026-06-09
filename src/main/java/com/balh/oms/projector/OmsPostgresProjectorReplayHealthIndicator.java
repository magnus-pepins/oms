package com.balh.oms.projector;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Readiness signal for the OMS→Postgres Aeron replay tail. Fails when the replay thread exits or
 * when the mirror cursor stops advancing while Archive replay backlog remains (wedge / apply stall).
 * Idle at the live tail (applied position == archive upper bound, empty apply queue) stays UP even
 * if no new orders arrive — unlike {@code oms.projector.lag_seconds}, which ages whenever
 * {@code last_applied_at} is quiet.
 */
@Component("omsPostgresProjectorReplay")
@Profile(OmsProfiles.POSTGRES_PROJECTOR)
@ConditionalOnProperty(prefix = "oms.cluster.projector", name = "enabled", havingValue = "true")
public class OmsPostgresProjectorReplayHealthIndicator implements HealthIndicator {

    private final OmsPostgresProjector projector;
    private final OmsConfig config;

    public OmsPostgresProjectorReplayHealthIndicator(OmsPostgresProjector projector, OmsConfig config) {
        this.projector = projector;
        this.config = config;
    }

    @Override
    public Health health() {
        if (!projector.isReplayLoopAlive()) {
            return Health.down()
                    .withDetail("reason", "oms-postgres-projector replay thread not running")
                    .withDetail("replayLoopRunningFlag", projector.isReplayLoopRunning())
                    .build();
        }

        long stallMs = projector.millisSinceLastCursorAdvance();
        long maxStallMs = config.getCluster().getProjector().getReadinessMaxReplayStallMs();
        boolean replayBacklog = projector.replayPositionHasBacklog();
        boolean applyBacklog = projector.applyQueueHasBacklog();

        if ((replayBacklog || applyBacklog) && stallMs > maxStallMs) {
            return Health.down()
                    .withDetail("reason", "projector cursor stalled while replay or apply backlog present")
                    .withDetail("appliedPosition", projector.lastAppliedPosition())
                    .withDetail("replayUpperBound", projector.liveReplayUpperBound())
                    .withDetail("replayBacklogFragments", projector.replayBacklogFragments())
                    .withDetail("applyQueueBacklog", applyBacklog)
                    .withDetail("stallMs", stallMs)
                    .withDetail("readinessMaxReplayStallMs", maxStallMs)
                    .build();
        }

        return Health.up()
                .withDetail("appliedPosition", projector.lastAppliedPosition())
                .withDetail("replayUpperBound", projector.liveReplayUpperBound())
                .withDetail("replayBacklogFragments", projector.replayBacklogFragments())
                .withDetail("applyQueueBacklog", applyBacklog)
                .withDetail("stallMs", stallMs)
                .build();
    }
}
