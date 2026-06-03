package com.balh.oms.venueegress;

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
 * Publishes {@value #GAUGE_NAME} on {@value OmsProfiles#VENUE_EGRESS} — wall-clock seconds since
 * {@code oms_venue_egress_cursor.last_applied_at} advanced. Mirror of
 * {@link com.balh.oms.fixegress.OmsFixEgressCursorLagPublisher}.
 */
@Component
@Profile(OmsProfiles.VENUE_EGRESS)
public class OmsVenueEgressCursorLagPublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsVenueEgressCursorLagPublisher.class);

    public static final String GAUGE_NAME = "oms.venue.egress.lag_seconds";
    public static final double NO_DATA_LAG_SECONDS = -1.0;

    private final OmsVenueEgressCursorRepository cursorRepository;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final String egressId;
    private final int streamId;

    private final AtomicReference<Instant> cachedLastAppliedAt = new AtomicReference<>();

    @Autowired
    public OmsVenueEgressCursorLagPublisher(
            OmsVenueEgressCursorRepository cursorRepository, MeterRegistry meterRegistry, Clock clock) {
        this(
                cursorRepository,
                meterRegistry,
                clock,
                OmsVenueEgressService.EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    OmsVenueEgressCursorLagPublisher(
            OmsVenueEgressCursorRepository cursorRepository,
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
        Gauge.builder(GAUGE_NAME, this, OmsVenueEgressCursorLagPublisher::currentLagSeconds)
                .description(
                        "Wall-clock seconds since oms_venue_egress_cursor.last_applied_at advanced."
                                + " -1 means no observation yet.")
                .tags(Tags.of("egress_id", egressId, "stream_id", Integer.toString(streamId)))
                .register(meterRegistry);
        log.info("Registered {} (egressId={}, streamId={})", GAUGE_NAME, egressId, streamId);
    }

    @Scheduled(fixedDelayString = "${oms.venue.egress.lag-poll-interval-ms:5000}")
    public void pollCursor() {
        try {
            cursorRepository
                    .findLastAppliedAt(egressId, streamId)
                    .ifPresent(cachedLastAppliedAt::set);
        } catch (RuntimeException e) {
            log.warn(
                    "{} poll failed (egressId={}, streamId={}): {}",
                    GAUGE_NAME,
                    egressId,
                    streamId,
                    e.toString());
        }
    }

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
