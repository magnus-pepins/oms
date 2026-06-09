package com.balh.oms.ingress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
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

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hardening follow-up from {@code plans/oms-internal-venue-and-prediction-market.md}: publishes
 * {@value #GAUGE_LAG_BYTES} on the order-accept JVM so operators can alert before
 * {@link VenueAdmissionGate} starts refusing {@code PREDMKT/*} accepts.
 *
 * <p><strong>Actionable lag.</strong> {@value #GAUGE_LAG_BYTES} is <em>excess</em> byte lag:
 * {@code max(0, raw_lag − pipelined_floor)} where {@code raw_lag} is
 * {@code projector_position − egress_position} on the shared events stream (recording-aware).
 * Pipelined egress ({@code venue-route-max-in-flight > 1}) legitimately trails the projector by
 * up to {@code in_flight × bytes_per_order}; subtracting that floor avoids treating a 250 KB
 * healthy in-flight window as a 93 MB wedge. {@link #GAUGE_RAW_LAG_BYTES} keeps the unadjusted
 * delta for soak scripts and deep dives.
 *
 * <p>{@link #currentHealthSnapshot()} is refreshed on the scheduler thread only; the accept hot path
 * reads the cached snapshot so cluster admit is not blocked by per-request Postgres cursor queries.
 */
@Component
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OmsVenueEgressLagPublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsVenueEgressLagPublisher.class);

    /** Excess byte lag beyond the pipelined in-flight floor — aligns with gate/throttle thresholds. */
    public static final String GAUGE_LAG_BYTES = "oms.venue.egress.lag_bytes";

    /** Raw projector−egress byte delta before pipelined-floor subtraction. */
    public static final String GAUGE_RAW_LAG_BYTES = "oms.venue.egress.raw_lag_bytes";

    /** Sentinel when egress or projector cursor is missing or on incomparable recordings. */
    public static final double NO_DATA_LAG_BYTES = -1.0;

    /**
     * Recording-aware lag reading shared by the gauge and {@link VenueAdmissionGate}.
     *
     * @param rawLagBytes projector−egress byte delta (non-negative when comparable)
     * @param pipelinedFloorBytes healthy in-flight window subtracted before gate decisions
     * @param excessLagBytes {@code max(0, rawLagBytes − pipelinedFloorBytes)}
     * @param measurable both cursors present and positions comparable
     */
    public record VenueEgressLagReading(
            long rawLagBytes, long pipelinedFloorBytes, long excessLagBytes, boolean measurable) {

        public static VenueEgressLagReading notMeasurable() {
            return new VenueEgressLagReading(0L, 0L, 0L, false);
        }
    }

    /**
     * Cached venue-egress health for {@link VenueAdmissionGate}. Updated by {@link #pollCursors()}
     * only — never on the HTTP accept hot path.
     */
    public record VenueEgressHealthSnapshot(
            boolean admissible,
            long throttleDelayNanos,
            String blockDetail,
            VenueEgressLagReading lagReading) {

        public static VenueEgressHealthSnapshot allow() {
            return new VenueEgressHealthSnapshot(
                    true, 0L, null, new VenueEgressLagReading(0L, 0L, 0L, true));
        }

        public static VenueEgressHealthSnapshot block(String detail, VenueEgressLagReading reading) {
            return new VenueEgressHealthSnapshot(false, 0L, detail, reading);
        }

        public boolean shouldThrottle() {
            return admissible && throttleDelayNanos > 0L;
        }
    }

    private final OmsConfig config;
    private final AeronProjectorCursorRepository projectorCursor;
    private final OmsVenueEgressCursorRepository venueEgressCursor;
    private final MeterRegistry meterRegistry;
    private final String egressId;
    private final int streamId;

    private final AtomicLong cachedExcessLagBytes = new AtomicLong((long) NO_DATA_LAG_BYTES);
    private final AtomicLong cachedRawLagBytes = new AtomicLong((long) NO_DATA_LAG_BYTES);
    private final AtomicReference<VenueEgressHealthSnapshot> cachedHealthSnapshot =
            new AtomicReference<>(VenueEgressHealthSnapshot.allow());
    private final AtomicInteger missingEgressConsecutivePolls = new AtomicInteger(0);
    private final AtomicLong missingEgressFirstSeenAtMs = new AtomicLong(0L);

    @Autowired
    public OmsVenueEgressLagPublisher(
            OmsConfig config,
            AeronProjectorCursorRepository projectorCursor,
            OmsVenueEgressCursorRepository venueEgressCursor,
            MeterRegistry meterRegistry) {
        this(
                config,
                projectorCursor,
                venueEgressCursor,
                meterRegistry,
                OmsVenueEgressService.EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    OmsVenueEgressLagPublisher(
            OmsConfig config,
            AeronProjectorCursorRepository projectorCursor,
            OmsVenueEgressCursorRepository venueEgressCursor,
            MeterRegistry meterRegistry,
            String egressId,
            int streamId) {
        this.config = config;
        this.projectorCursor = projectorCursor;
        this.venueEgressCursor = venueEgressCursor;
        this.meterRegistry = meterRegistry;
        this.egressId = egressId;
        this.streamId = streamId;
    }

    @PostConstruct
    void registerGauge() {
        Tags tags = Tags.of("egress_id", egressId, "stream_id", Integer.toString(streamId));
        Gauge.builder(GAUGE_LAG_BYTES, this, OmsVenueEgressLagPublisher::currentLagBytes)
                .description(
                        "Excess OMS cluster events bytes the venue egress trails the postgres projector"
                                + " beyond the pipelined in-flight floor (max(0, raw − floor)). -1 when not measurable.")
                .tags(tags)
                .register(meterRegistry);
        Gauge.builder(GAUGE_RAW_LAG_BYTES, this, OmsVenueEgressLagPublisher::currentRawLagBytes)
                .description(
                        "Raw projector_pos − egress_pos byte delta before pipelined-floor subtraction."
                                + " -1 when not measurable.")
                .tags(tags)
                .register(meterRegistry);
        log.info(
                "Registered {} and {} gauges (egressId={}, streamId={})",
                GAUGE_LAG_BYTES,
                GAUGE_RAW_LAG_BYTES,
                egressId,
                streamId);
        pollCursors();
    }

    @Scheduled(fixedDelayString = "${oms.venue.egress.lag-poll-interval-ms:5000}")
    public void pollCursors() {
        try {
            VenueEgressLagReading reading = evaluateLagReading();
            VenueEgressHealthSnapshot previous = cachedHealthSnapshot.get();
            if (reading.measurable()) {
                cachedExcessLagBytes.set(reading.excessLagBytes());
                cachedRawLagBytes.set(reading.rawLagBytes());
            } else {
                cachedExcessLagBytes.set((long) NO_DATA_LAG_BYTES);
                cachedRawLagBytes.set((long) NO_DATA_LAG_BYTES);
            }
            VenueEgressHealthSnapshot next = computeHealthSnapshot(reading);
            cachedHealthSnapshot.set(next);
            if (!next.admissible()
                    && (previous.admissible() || !Objects.equals(previous.blockDetail(), next.blockDetail()))) {
                log.warn(
                        "venue admission snapshot blocked (egressId={}, streamId={}, reason={})",
                        egressId,
                        streamId,
                        next.blockDetail());
            }
        } catch (RuntimeException e) {
            log.warn(
                    "{} poll failed (egressId={}, streamId={}): {}; gauge and gate snapshot keep last value",
                    GAUGE_LAG_BYTES,
                    egressId,
                    streamId,
                    e.toString());
        }
    }

    /** Excess lag (actionable) — see class javadoc. */
    public double currentLagBytes() {
        return cachedExcessLagBytes.get();
    }

    /** Raw projector−egress delta before pipelined-floor subtraction. */
    public double currentRawLagBytes() {
        return cachedRawLagBytes.get();
    }

    /** Last polled health verdict; safe to read from the accept hot path (no I/O). */
    public VenueEgressHealthSnapshot currentHealthSnapshot() {
        return cachedHealthSnapshot.get();
    }

    VenueEgressLagReading evaluateLagReading() {
        OmsConfig.Venue.AdmissionGate gate = config.getVenue().getAdmissionGate();
        int venueRouteMaxInFlight = config.getCluster().getVenueEgress().getVenueRouteMaxInFlight();
        long pipelinedFloor = gate.pipelinedFloorBytes(venueRouteMaxInFlight);

        OptionalLong egressPos = venueEgressCursor.findLastAppliedPosition(egressId, streamId);
        OptionalLong projectorPos =
                projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, streamId);
        if (egressPos.isEmpty() || projectorPos.isEmpty()) {
            return VenueEgressLagReading.notMeasurable();
        }

        Optional<AeronProjectorCursorRepository.RecordedCursor> projCursor =
                projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, streamId);
        Optional<OmsVenueEgressCursorRepository.RecordedCursor> egrCursor =
                venueEgressCursor.findLastAppliedCursor(egressId, streamId);
        if (projCursor.isPresent()
                && egrCursor.isPresent()
                && projCursor.get().hasRecordingId()
                && egrCursor.get().hasRecordingId()
                && egrCursor.get().recordingId() != projCursor.get().recordingId()) {
            if (egrCursor.get().recordingId() < projCursor.get().recordingId()) {
                return new VenueEgressLagReading(Long.MAX_VALUE, pipelinedFloor, Long.MAX_VALUE, true);
            }
            return new VenueEgressLagReading(0L, pipelinedFloor, 0L, true);
        }

        long rawLag = projectorPos.getAsLong() - egressPos.getAsLong();
        if (rawLag < 0L) {
            rawLag = 0L;
        }
        long excess = Math.max(0L, rawLag - pipelinedFloor);
        return new VenueEgressLagReading(rawLag, pipelinedFloor, excess, true);
    }

    long computeLagBytes() {
        VenueEgressLagReading reading = evaluateLagReading();
        return reading.measurable() ? reading.excessLagBytes() : (long) NO_DATA_LAG_BYTES;
    }

    VenueEgressHealthSnapshot computeHealthSnapshot() {
        return computeHealthSnapshot(evaluateLagReading());
    }

    VenueEgressHealthSnapshot computeHealthSnapshot(VenueEgressLagReading reading) {
        OmsConfig.Venue.AdmissionGate gate = config.getVenue().getAdmissionGate();
        if (!gate.isEnabled()) {
            resetMissingEgressHysteresis();
            return VenueEgressHealthSnapshot.allow();
        }

        OptionalLong egressPos = venueEgressCursor.findLastAppliedPosition(egressId, streamId);
        OptionalLong projectorPos =
                projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, streamId);

        if (egressPos.isEmpty() && projectorPos.isEmpty()) {
            resetMissingEgressHysteresis();
            return VenueEgressHealthSnapshot.allow();
        }
        if (egressPos.isEmpty()) {
            long nowMs = System.currentTimeMillis();
            long firstSeenAtMs = missingEgressFirstSeenAtMs.updateAndGet(prev -> prev == 0L ? nowMs : prev);
            int consecutivePolls = missingEgressConsecutivePolls.incrementAndGet();
            long elapsedMs = Math.max(0L, nowMs - firstSeenAtMs);
            boolean blockByPolls = consecutivePolls >= gate.getMissingEgressBlockedPolls();
            boolean blockByGrace = elapsedMs >= gate.getMissingEgressGraceMs();
            if (!blockByPolls && !blockByGrace) {
                return VenueEgressHealthSnapshot.allow();
            }
            return VenueEgressHealthSnapshot.block(
                    "venue egress cursor absent while projector is live"
                            + " (consecutive_polls="
                            + consecutivePolls
                            + "/"
                            + gate.getMissingEgressBlockedPolls()
                            + ", elapsed_ms="
                            + elapsedMs
                            + "/"
                            + gate.getMissingEgressGraceMs()
                            + ")",
                    reading);
        }
        if (projectorPos.isEmpty()) {
            resetMissingEgressHysteresis();
            return VenueEgressHealthSnapshot.allow();
        }
        resetMissingEgressHysteresis();

        if (!reading.measurable()) {
            return VenueEgressHealthSnapshot.allow();
        }

        Optional<AeronProjectorCursorRepository.RecordedCursor> projCursor =
                projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, streamId);
        Optional<OmsVenueEgressCursorRepository.RecordedCursor> egrCursor =
                venueEgressCursor.findLastAppliedCursor(egressId, streamId);
        if (projCursor.isPresent()
                && egrCursor.isPresent()
                && projCursor.get().hasRecordingId()
                && egrCursor.get().hasRecordingId()
                && egrCursor.get().recordingId() != projCursor.get().recordingId()) {
            if (egrCursor.get().recordingId() < projCursor.get().recordingId()) {
                return VenueEgressHealthSnapshot.block(
                        "venue egress on older recording " + egrCursor.get().recordingId()
                                + " < projector recording " + projCursor.get().recordingId(),
                        reading);
            }
            // Egress rolled forward through a completed / empty tombstone recording while the
            // projector is still replaying the prior one (observed on pop 2026-06-08: egress
            // on recording 123, projector on 0 @ 72MB). Allowing accepts here produced
            // PENDING_NEW rows with no projector mirror and a wedged customer UI.
            return VenueEgressHealthSnapshot.block(
                    "venue egress on newer recording " + egrCursor.get().recordingId()
                            + " > projector recording " + projCursor.get().recordingId()
                            + " (journal diverged; wait for projector to roll forward)",
                    reading);
        }

        // Projector must not trail egress on the same recording. Observed on pop 2026-06-08 when
        // oms-postgres-projector replay stopped tailing while egress continued (e.g. projector @
        // 1952, egress @ 2624). evaluateLagReading() clamps negative rawLag to 0, so the
        // excess-lag check below would still allow accepts — customers got HTTP 201 + locked funds
        // but no orders row, no NATS events, and a permanent "Awaiting Approval" UI.
        if (projCursor.isPresent()
                && egrCursor.isPresent()
                && projCursor.get().hasRecordingId()
                && egrCursor.get().hasRecordingId()
                && egrCursor.get().recordingId() == projCursor.get().recordingId()
                && egressPos.getAsLong() > projectorPos.getAsLong()) {
            return VenueEgressHealthSnapshot.block(
                    "projector trailing venue egress on recording "
                            + projCursor.get().recordingId()
                            + " (projector="
                            + projectorPos.getAsLong()
                            + " egress="
                            + egressPos.getAsLong()
                            + "; projector replay likely wedged)",
                    reading);
        }

        long excessLag = reading.excessLagBytes();
        long maxExcessLag = gate.getMaxLagBytes();
        if (excessLag > maxExcessLag) {
            return VenueEgressHealthSnapshot.block(
                    "venue egress excess lag " + excessLag + " bytes exceeds max " + maxExcessLag
                            + " (raw="
                            + reading.rawLagBytes()
                            + " floor="
                            + reading.pipelinedFloorBytes()
                            + " projector="
                            + projectorPos.getAsLong()
                            + " egress="
                            + egressPos.getAsLong()
                            + ")",
                    reading);
        }

        long throttleDelay = gate.throttleDelayNanos(excessLag);
        if (throttleDelay > 0L) {
            return new VenueEgressHealthSnapshot(
                    true,
                    throttleDelay,
                    null,
                    reading);
        }
        return new VenueEgressHealthSnapshot(true, 0L, null, reading);
    }

    private void resetMissingEgressHysteresis() {
        missingEgressConsecutivePolls.set(0);
        missingEgressFirstSeenAtMs.set(0L);
    }
}
