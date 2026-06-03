package com.balh.oms.ingress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.projector.AeronProjectorCursorRepository;
import com.balh.oms.projector.OmsPostgresProjector;
import com.balh.oms.venueegress.OmsVenueEgressCursorRepository;
import com.balh.oms.venueegress.OmsVenueEgressService;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hardening follow-up from {@code plans/oms-internal-venue-and-prediction-market.md}: publishes
 * {@value #GAUGE_LAG_BYTES} on the order-accept JVM so operators can alert before
 * {@link VenueAdmissionGate} starts refusing {@code PREDMKT/*} accepts.
 *
 * <p>Lag is {@code projector_position - egress_position} on the shared OMS cluster events stream,
 * using the same recording-aware comparison as the gate. Returns {@link #NO_DATA_LAG_BYTES} until
 * both cursors exist and are comparable.
 */
@Component
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OmsVenueEgressLagPublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsVenueEgressLagPublisher.class);

    public static final String GAUGE_LAG_BYTES = "oms.venue.egress.lag_bytes";

    /** Sentinel when egress or projector cursor is missing or on incomparable recordings. */
    public static final double NO_DATA_LAG_BYTES = -1.0;

    private final AeronProjectorCursorRepository projectorCursor;
    private final OmsVenueEgressCursorRepository venueEgressCursor;
    private final MeterRegistry meterRegistry;
    private final String egressId;
    private final int streamId;

    private final AtomicLong cachedLagBytes = new AtomicLong((long) NO_DATA_LAG_BYTES);

    @Autowired
    public OmsVenueEgressLagPublisher(
            AeronProjectorCursorRepository projectorCursor,
            OmsVenueEgressCursorRepository venueEgressCursor,
            MeterRegistry meterRegistry) {
        this(
                projectorCursor,
                venueEgressCursor,
                meterRegistry,
                OmsVenueEgressService.EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    OmsVenueEgressLagPublisher(
            AeronProjectorCursorRepository projectorCursor,
            OmsVenueEgressCursorRepository venueEgressCursor,
            MeterRegistry meterRegistry,
            String egressId,
            int streamId) {
        this.projectorCursor = projectorCursor;
        this.venueEgressCursor = venueEgressCursor;
        this.meterRegistry = meterRegistry;
        this.egressId = egressId;
        this.streamId = streamId;
    }

    @PostConstruct
    void registerGauge() {
        Gauge.builder(GAUGE_LAG_BYTES, this, OmsVenueEgressLagPublisher::currentLagBytes)
                .description(
                        "OMS cluster events bytes the venue egress trails the postgres projector"
                                + " (projector_pos - egress_pos). -1 when not measurable.")
                .tags(Tags.of("egress_id", egressId, "stream_id", Integer.toString(streamId)))
                .register(meterRegistry);
        log.info("Registered {} gauge (egressId={}, streamId={})", GAUGE_LAG_BYTES, egressId, streamId);
    }

    @Scheduled(fixedDelayString = "${oms.venue.egress.lag-poll-interval-ms:5000}")
    public void pollCursors() {
        try {
            cachedLagBytes.set(computeLagBytes());
        } catch (RuntimeException e) {
            log.warn(
                    "{} poll failed (egressId={}, streamId={}): {}; gauge keeps last value",
                    GAUGE_LAG_BYTES,
                    egressId,
                    streamId,
                    e.toString());
        }
    }

    public double currentLagBytes() {
        return cachedLagBytes.get();
    }

    long computeLagBytes() {
        OptionalLong egressPos = venueEgressCursor.findLastAppliedPosition(egressId, streamId);
        OptionalLong projectorPos =
                projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, streamId);
        if (egressPos.isEmpty() || projectorPos.isEmpty()) {
            return (long) NO_DATA_LAG_BYTES;
        }

        java.util.Optional<AeronProjectorCursorRepository.RecordedCursor> projCursor =
                projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, streamId);
        java.util.Optional<OmsVenueEgressCursorRepository.RecordedCursor> egrCursor =
                venueEgressCursor.findLastAppliedCursor(egressId, streamId);
        if (projCursor.isPresent()
                && egrCursor.isPresent()
                && projCursor.get().hasRecordingId()
                && egrCursor.get().hasRecordingId()
                && egrCursor.get().recordingId() != projCursor.get().recordingId()) {
            if (egrCursor.get().recordingId() < projCursor.get().recordingId()) {
                return Long.MAX_VALUE;
            }
            return 0L;
        }

        long lag = projectorPos.getAsLong() - egressPos.getAsLong();
        return lag < 0 ? 0L : lag;
    }
}
