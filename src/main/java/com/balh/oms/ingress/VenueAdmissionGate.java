package com.balh.oms.ingress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.projector.AeronProjectorCursorRepository;
import com.balh.oms.projector.OmsPostgresProjector;
import com.balh.oms.routing.VenueRoutingSymbols;
import com.balh.oms.venueegress.OmsVenueEgressCursorRepository;
import com.balh.oms.venueegress.OmsVenueEgressService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Pre-admission circuit breaker for venue-routed orders.
 *
 * <p><b>Why this exists.</b> The OMS cluster admits an order the instant {@code OrderIngressService}
 * gets an {@code OrderAccepted} back — independent of whether {@code oms-venue-egress} can forward it
 * to {@code balh-venue}. On 2026-06-03 the egress wedged (gRPC stuck on a back-pressured venue leader,
 * see {@link com.balh.oms.venueegress.OmsVenueEgressService} + {@code VenueRouteOrderClient}); the OMS
 * kept admitting {@code PREDMKT/*} orders that piled up as WORKING-in-OMS-but-invisible-at-the-venue
 * for ~3 500 events. The customer saw stale odds and orders that never reached the book. The operator's
 * instruction: <em>"we cannot continue accepting orders when something like this happens."</em>
 *
 * <p><b>What it does.</b> On the accept path (HTTP {@code POST /…/orders} and the gRPC ingress, both via
 * {@link OrderIngressService#persistAccepted}), for symbols routed to the internal venue (prefix match,
 * default {@code PREDMKT}), it compares the projector cursor and the venue-egress cursor on the shared
 * cluster events recording. If the egress is on an older Aeron Archive recording than the projector, or
 * trails it by more than {@code oms.venue.admission-gate.max-lag-bytes}, or has never run at all, the
 * accept is refused with HTTP 503 {@code venue_unavailable}. Equities / FIX-routed orders are never
 * gated — they short-circuit before any query.
 *
 * <p><b>Bounds.</b> The position-lag signal can only let through orders admitted in the window between a
 * sudden egress death and the next accept (the projector advances, the egress does not, so the very next
 * venue-routed accept trips). It cannot be tighter than one order without a live egress heartbeat; that
 * is an accepted trade-off versus the unbounded backlog the pre-gate code allowed. Only venue-routed
 * accepts query Postgres (two indexed point-selects); PREDMKT is low-volume retail flow.
 */
@Component
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class VenueAdmissionGate {

    private static final Logger log = LoggerFactory.getLogger(VenueAdmissionGate.class);

    /** Reject code surfaced to the BFF / FIX client; HTTP 503. */
    static final String REJECT_CODE = "venue_unavailable";

    private static final String METRIC_BLOCKED = "oms_venue_admission_gate_blocked_total";

    private final OmsConfig config;
    private final AeronProjectorCursorRepository projectorCursor;
    private final OmsVenueEgressCursorRepository venueEgressCursor;
    private final MeterRegistry meterRegistry;

    public VenueAdmissionGate(
            OmsConfig config,
            AeronProjectorCursorRepository projectorCursor,
            OmsVenueEgressCursorRepository venueEgressCursor,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.projectorCursor = projectorCursor;
        this.venueEgressCursor = venueEgressCursor;
        this.meterRegistry = meterRegistry;
    }

    /**
     * @throws ClusterAdmissionException HTTP 503 {@code venue_unavailable} when the symbol is
     *     venue-routed and the egress pipeline is unhealthy. No-op for non-venue symbols, when the
     *     gate is disabled, or when the egress is caught up.
     */
    public void assertVenueAdmissible(String instrumentSymbol) {
        OmsConfig.Venue.AdmissionGate gate = config.getVenue().getAdmissionGate();
        if (!gate.isEnabled()) {
            return;
        }
        if (!isVenueRouted(instrumentSymbol)) {
            return;
        }

        int streamId = OmsClusterWireFormat.EVENTS_STREAM_ID;

        OptionalLong egressPos =
                venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, streamId);
        OptionalLong projectorPos =
                projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, streamId);

        if (egressPos.isEmpty() && projectorPos.isEmpty()) {
            // Fresh stack (e.g. post-wipe): neither consumer has persisted a cursor yet and no
            // events have been admitted. There is no lag to measure — allow the first admits.
            return;
        }
        if (egressPos.isEmpty()) {
            trip(instrumentSymbol, "venue egress cursor absent — oms-venue-egress has never applied an event");
        }
        if (projectorPos.isEmpty()) {
            // Egress is live but the projector has not applied yet — egress cannot be behind.
            return;
        }

        // Recording-aware guard: a position is only comparable within one Aeron Archive recording.
        // If the egress is stuck on an older recording than the projector it is definitively behind,
        // regardless of byte position (see AeronProjectorCursorRepository class Javadoc / V55).
        Optional<AeronProjectorCursorRepository.RecordedCursor> projCursor =
                projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, streamId);
        Optional<OmsVenueEgressCursorRepository.RecordedCursor> egrCursor =
                venueEgressCursor.findLastAppliedCursor(OmsVenueEgressService.EGRESS_ID, streamId);
        if (projCursor.isPresent()
                && egrCursor.isPresent()
                && projCursor.get().hasRecordingId()
                && egrCursor.get().hasRecordingId()
                && egrCursor.get().recordingId() != projCursor.get().recordingId()) {
            if (egrCursor.get().recordingId() < projCursor.get().recordingId()) {
                trip(
                        instrumentSymbol,
                        "venue egress on older recording " + egrCursor.get().recordingId()
                                + " < projector recording " + projCursor.get().recordingId());
            }
            // Egress recording > projector recording: egress is ahead of the projector's view, so the
            // venue is not the bottleneck. Treat as healthy (projector lag is a separate concern).
            return;
        }

        long lagBytes = projectorPos.getAsLong() - egressPos.getAsLong();
        long maxLagBytes =
                gate.effectiveMaxLagBytes(config.getCluster().getVenueEgress().getVenueRouteMaxInFlight());
        if (lagBytes > maxLagBytes) {
            trip(
                    instrumentSymbol,
                    "venue egress lag " + lagBytes + " bytes exceeds max " + maxLagBytes
                            + " (projector=" + projectorPos.getAsLong() + " egress=" + egressPos.getAsLong() + ")");
        }
    }

    private boolean isVenueRouted(String instrumentSymbol) {
        // Decoupled from oms.routing.venue-symbol-prefix-routing-enabled (that toggle lives on the
        // egress JVMs, not the ingress JVM): the gate always identifies venue symbols by prefix so
        // it works regardless of the ingress routing config.
        return VenueRoutingSymbols.matchesVenuePrefix(
                config.getRouting().getVenueSymbolPrefix(), instrumentSymbol);
    }

    private void trip(String instrumentSymbol, String detail) {
        Counter.builder(METRIC_BLOCKED)
                .description("Venue-routed accepts refused because oms-venue-egress is unhealthy")
                .tag("symbol", instrumentSymbol == null ? "" : instrumentSymbol)
                .register(meterRegistry)
                .increment();
        log.error("venue admission gate tripped symbol={} reason={}", instrumentSymbol, detail);
        throw new ClusterAdmissionException(
                HttpStatus.SERVICE_UNAVAILABLE,
                REJECT_CODE,
                "venue temporarily not accepting orders for " + instrumentSymbol + ": " + detail);
    }
}
