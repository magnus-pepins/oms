package com.balh.oms.projector;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsProfiles;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase 4 slice 4d of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: publishes
 * {@code oms.projector.lag_seconds} on the {@value OmsProfiles#POSTGRES_PROJECTOR} JVM.
 *
 * <p>Source of truth is the {@code aeron_projector_cursor.last_applied_at} column (server-side
 * {@code NOW()} on every {@link AeronProjectorCursorRepository#advance}). A {@link Scheduled} poll
 * caches the value in an {@link AtomicReference} and the Micrometer {@link Gauge} returns
 * {@code now() - cached} in seconds on each scrape. The cache shields scrape-time from a flaky
 * Postgres connection — if the poll fails the gauge keeps returning the last value, which then
 * ages and grows, which is the alert-worthy signal we want.
 *
 * <p>Cold-start sentinel: until the first successful poll observes a row, the gauge returns
 * {@link #NO_DATA_LAG_SECONDS} ({@value #NO_DATA_LAG_SECONDS}). Operators alerting on lag should
 * filter the sentinel out (e.g. {@code oms_projector_lag_seconds > 30 unless oms_projector_lag_seconds == -1});
 * a separate {@code absent(...)} alert covers the "no metric at all" case.
 */
@Component
@Profile(OmsProfiles.POSTGRES_PROJECTOR)
public class OmsProjectorCursorLagPublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsProjectorCursorLagPublisher.class);

    public static final String GAUGE_NAME = "oms.projector.lag_seconds";

    public static final String REPLAY_BACKLOG_GAUGE_NAME = "oms.projector.replay_backlog_fragments";

    /** Sentinel returned by the gauge before any successful poll. See class javadoc. */
    public static final double NO_DATA_LAG_SECONDS = -1.0;

    private final AeronProjectorCursorRepository cursorRepository;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final OmsPostgresProjector projector;
    private final String projectorId;
    private final int streamId;

    private final AtomicReference<Instant> cachedLastAppliedAt = new AtomicReference<>();

    @Autowired
    public OmsProjectorCursorLagPublisher(
            AeronProjectorCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            Clock clock,
            OmsPostgresProjector projector) {
        this(
                cursorRepository,
                meterRegistry,
                clock,
                projector,
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    /**
     * Test-only constructor — accepts custom {@code projectorId} / {@code streamId}. Spring
     * ignores this overload because the {@link Autowired @Autowired}-marked one above is the
     * only candidate.
     */
    OmsProjectorCursorLagPublisher(
            AeronProjectorCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            Clock clock,
            OmsPostgresProjector projector,
            String projectorId,
            int streamId) {
        this.cursorRepository = cursorRepository;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        this.projector = projector;
        this.projectorId = projectorId;
        this.streamId = streamId;
    }

    @PostConstruct
    void registerGauge() {
        Gauge.builder(GAUGE_NAME, this, OmsProjectorCursorLagPublisher::currentLagSeconds)
                .description(
                        "Wall-clock seconds since the projector cursor (aeron_projector_cursor.last_applied_at)"
                                + " last advanced. -1 means no observation yet.")
                .tags(Tags.of("projector_id", projectorId, "stream_id", Integer.toString(streamId)))
                .register(meterRegistry);
        Gauge.builder(REPLAY_BACKLOG_GAUGE_NAME, projector, OmsPostgresProjector::replayBacklogFragments)
                .description(
                        "Archive replay fragments not yet applied (upperBound - appliedPosition)."
                                + " Zero at the live tail; rises during catch-up or wedge.")
                .tags(Tags.of("projector_id", projectorId, "stream_id", Integer.toString(streamId)))
                .register(meterRegistry);
        log.info(
                "Registered {} and {} gauges (projectorId={}, streamId={})",
                GAUGE_NAME,
                REPLAY_BACKLOG_GAUGE_NAME,
                projectorId,
                streamId);
    }

    /**
     * Polled cadence is short enough to keep the gauge close to real-time but loose enough to keep
     * Postgres load trivial — at 5 s × 1 SELECT this is &lt;0.001 % of typical Postgres budget.
     */
    @Scheduled(fixedDelayString = "${oms.projector.lag-poll-interval-ms:5000}")
    public void pollCursor() {
        try {
            cursorRepository
                    .findLastAppliedAt(projectorId, streamId)
                    .ifPresent(cachedLastAppliedAt::set);
        } catch (RuntimeException e) {
            log.warn(
                    "{} poll failed (projectorId={}, streamId={}): {}; gauge will keep last value",
                    GAUGE_NAME,
                    projectorId,
                    streamId,
                    e.toString());
        }
    }

    /** Visible for tests + the gauge supplier. Returns {@link #NO_DATA_LAG_SECONDS} pre-first-poll. */
    public double currentLagSeconds() {
        Instant cached = cachedLastAppliedAt.get();
        if (cached == null) {
            return NO_DATA_LAG_SECONDS;
        }
        Duration delta = Duration.between(cached, clock.instant());
        if (delta.isNegative()) {
            return 0.0;
        }
        return delta.toMillis() / 1_000.0;
    }
}
