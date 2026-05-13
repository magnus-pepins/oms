package com.balh.oms.fixegress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsProfiles;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase 4 slice 4d of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: publishes
 * {@code oms.fix_egress.lag_seconds} on the {@value OmsProfiles#FIX_EGRESS} JVM.
 *
 * <p>Mirror of {@link com.balh.oms.projector.OmsProjectorCursorLagPublisher}; reads
 * {@code oms_fix_egress_cursor.last_applied_at}. See the projector publisher's javadoc for the
 * caching / cold-start / poll-failure semantics — they apply here unchanged.
 */
@Component
@Profile(OmsProfiles.FIX_EGRESS)
public class OmsFixEgressCursorLagPublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsFixEgressCursorLagPublisher.class);

    public static final String GAUGE_NAME = "oms.fix_egress.lag_seconds";

    /** Sentinel returned by the gauge before any successful poll. */
    public static final double NO_DATA_LAG_SECONDS = -1.0;

    private final OmsFixEgressCursorRepository cursorRepository;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final String egressId;
    private final int streamId;

    private final AtomicReference<Instant> cachedLastAppliedAt = new AtomicReference<>();

    public OmsFixEgressCursorLagPublisher(
            OmsFixEgressCursorRepository cursorRepository, MeterRegistry meterRegistry, Clock clock) {
        this(
                cursorRepository,
                meterRegistry,
                clock,
                OmsFixEgressService.EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    /** Test-only constructor — accepts custom {@code egressId} / {@code streamId}. */
    OmsFixEgressCursorLagPublisher(
            OmsFixEgressCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            Clock clock,
            String egressId,
            int streamId) {
        this.cursorRepository = cursorRepository;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        this.egressId = egressId;
        this.streamId = streamId;
    }

    @PostConstruct
    void registerGauge() {
        Gauge.builder(GAUGE_NAME, this, OmsFixEgressCursorLagPublisher::currentLagSeconds)
                .description(
                        "Wall-clock seconds since the FIX-egress cursor (oms_fix_egress_cursor.last_applied_at)"
                                + " last advanced. -1 means no observation yet.")
                .tags(Tags.of("egress_id", egressId, "stream_id", Integer.toString(streamId)))
                .register(meterRegistry);
        log.info("Registered {} gauge (egressId={}, streamId={})", GAUGE_NAME, egressId, streamId);
    }

    @Scheduled(fixedDelayString = "${oms.fix-egress.lag-poll-interval-ms:5000}")
    public void pollCursor() {
        try {
            cursorRepository
                    .findLastAppliedAt(egressId, streamId)
                    .ifPresent(cachedLastAppliedAt::set);
        } catch (RuntimeException e) {
            log.warn(
                    "{} poll failed (egressId={}, streamId={}): {}; gauge will keep last value",
                    GAUGE_NAME,
                    egressId,
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
