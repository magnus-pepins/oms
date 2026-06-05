package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.routing.VenueRoutingSymbols;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.LockSupport;

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
 * default {@code PREDMKT}), it consults the cached venue-egress health snapshot from
 * {@link OmsVenueEgressLagPublisher} (refreshed on a scheduler thread, default every 5 s). Decisions
 * use <em>excess</em> byte lag ({@code raw − pipelined_floor}) so a healthy pipelined in-flight window
 * does not look like a wedge. When excess lag sits between {@code throttle-excess-lag-bytes} and
 * {@code max-lag-bytes}, the gate applies a bounded accept-path delay (soft backpressure) before the
 * cluster admit proceeds. When excess lag exceeds {@code max-lag-bytes}, or the egress is on an older
 * Aeron Archive recording than the projector, or has never run at all, the accept is refused with HTTP
 * 503 {@code venue_unavailable}. Equities / FIX-routed orders are never gated — they short-circuit
 * before any snapshot read.
 *
 * <p><b>Hot path.</b> Cluster admit is <em>not</em> blocked by per-request Postgres cursor queries; the
 * gate reads only the in-memory snapshot so burst venue accepts do not contend on the projector DB for
 * lag measurement. True overload (wedged egress) still surfaces 503 once the scheduled poll observes it.
 *
 * <p><b>Bounds.</b> The position-lag signal can only let through orders admitted in the window between a
 * sudden egress death and the next poll (the projector advances, the egress does not, so the very next
 * venue-routed accept after poll may trip). It cannot be tighter than one order without a live egress
 * heartbeat; that is an accepted trade-off versus the unbounded backlog the pre-gate code allowed. Only
 * venue-routed accepts read the snapshot; PREDMKT is low-volume retail flow.
 */
@Component
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class VenueAdmissionGate {

    private static final Logger log = LoggerFactory.getLogger(VenueAdmissionGate.class);

    /** Reject code surfaced to the BFF / FIX client; HTTP 503. */
    static final String REJECT_CODE = "venue_unavailable";

    private static final String METRIC_BLOCKED = "oms_venue_admission_gate_blocked_total";
    private static final String METRIC_THROTTLED = "oms_venue_admission_gate_throttled_total";

    private final OmsConfig config;
    private final OmsVenueEgressLagPublisher venueEgressLagPublisher;
    private final MeterRegistry meterRegistry;

    public VenueAdmissionGate(
            OmsConfig config,
            OmsVenueEgressLagPublisher venueEgressLagPublisher,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.venueEgressLagPublisher = venueEgressLagPublisher;
        this.meterRegistry = meterRegistry;
    }

    /**
     * @throws ClusterAdmissionException HTTP 503 {@code venue_unavailable} when the symbol is
     *     venue-routed and the cached egress pipeline health is unhealthy. No-op for non-venue
     *     symbols, when the gate is disabled, or when the egress is caught up.
     */
    public void assertVenueAdmissible(String instrumentSymbol) {
        if (!config.getVenue().getAdmissionGate().isEnabled()) {
            return;
        }
        if (!isVenueRouted(instrumentSymbol)) {
            return;
        }

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                venueEgressLagPublisher.currentHealthSnapshot();
        if (snapshot.shouldThrottle()) {
            Counter.builder(METRIC_THROTTLED)
                    .description(
                            "Venue-routed accepts delayed by soft backpressure while egress excess lag"
                                    + " is above the throttle floor but below hard 503 threshold")
                    .tag("symbol", instrumentSymbol == null ? "" : instrumentSymbol)
                    .register(meterRegistry)
                    .increment();
            LockSupport.parkNanos(snapshot.throttleDelayNanos());
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
            }
        }
        if (!snapshot.admissible()) {
            trip(instrumentSymbol, snapshot.blockDetail());
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
