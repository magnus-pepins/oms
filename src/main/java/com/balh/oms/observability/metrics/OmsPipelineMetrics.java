package com.balh.oms.observability.metrics;

import com.balh.oms.cluster.AcceptOrderCommand;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Centralised Micrometer recording for the ingress accept timer (Prometheus via
 * {@code /actuator/prometheus}). The Phase-1 control-apply / fix-outbound / ingress-to-fix-nos
 * recorders were removed in slice 4i once their producers had been deleted across Phase 3.
 *
 * <p>Phase 4j adds the cross-JVM admit-to-NOS / admit-to-projector histograms; see
 * {@link #recordClusterAdmitToFixNos} and {@link #recordClusterAdmitToProjector}.
 */
public final class OmsPipelineMetrics {

    /** Tag key: side (buy / sell). */
    public static final String TAG_SIDE = "side";

    /** Tag key: time-in-force code translated to FIX/Postgres canonical string. */
    public static final String TAG_TIF = "tif";

    /** Tag key for the egress identity ({@code OmsFixEgressService.EGRESS_ID}). */
    public static final String TAG_EGRESS_ID = "egress_id";

    /** Tag key for the projector identity (matches {@code OmsPostgresProjector.PROJECTOR_ID}). */
    public static final String TAG_PROJECTOR_ID = "projector_id";

    private OmsPipelineMetrics() {}

    public static void finishIngressAccept(MeterRegistry registry, Timer.Sample sample, String outcome) {
        sample.stop(baseTimer(OmsPipelineMeterNames.INGRESS_ACCEPT)
                .description(
                        "Postgres transaction for internal order accept until commit (domain_event_outbox + optional ledger_inflight_outbox); orders writes moved to OmsPostgresProjector in slice 2c")
                .tag("outcome", outcome)
                .register(registry));
    }

    /**
     * Phase 4j: record the cluster-admit -> NOS-on-wire latency for one
     * {@link com.balh.oms.cluster.OrderAdmittedEvent} on the fix-egress JVM. Caller passes
     * {@code latencyMs = System.currentTimeMillis() - ev.acceptedAtMillis()}; this method clamps
     * negative values (rare NTP-slew-backwards) to zero so the Timer never receives a
     * negative duration.
     */
    public static void recordClusterAdmitToFixNos(
            MeterRegistry registry, String egressId, byte sideCode, byte tifCode, long latencyMs) {
        long clamped = Math.max(0L, latencyMs);
        baseTimer(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_FIX_NOS)
                .description(
                        "Wall-clock ms from OrderAdmittedEvent.acceptedAtMillis (cluster admit) until"
                                + " Session.sendToTarget(NewOrderSingle) returned successfully on the fix-egress JVM."
                                + " Recorded once per fragment, immediately before the oms_fix_egress_cursor advance.")
                .tag(TAG_EGRESS_ID, egressId)
                .tag(TAG_SIDE, sideLabel(sideCode))
                .tag(TAG_TIF, tifLabel(tifCode))
                .register(registry)
                .record(Duration.ofMillis(clamped));
    }

    /**
     * Phase 4j: mirror of {@link #recordClusterAdmitToFixNos} on the postgres-projector JVM.
     * Caller passes {@code latencyMs = System.currentTimeMillis() - ev.acceptedAtMillis()} after
     * the {@code orders} UPSERT + control admission transaction commits.
     */
    public static void recordClusterAdmitToProjector(
            MeterRegistry registry, String projectorId, byte sideCode, byte tifCode, long latencyMs) {
        long clamped = Math.max(0L, latencyMs);
        baseTimer(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_PROJECTOR)
                .description(
                        "Wall-clock ms from OrderAdmittedEvent.acceptedAtMillis (cluster admit) until the"
                                + " postgres-projector finished its orders UPSERT + control admission transaction.")
                .tag(TAG_PROJECTOR_ID, projectorId)
                .tag(TAG_SIDE, sideLabel(sideCode))
                .tag(TAG_TIF, tifLabel(tifCode))
                .register(registry)
                .record(Duration.ofMillis(clamped));
    }

    private static String sideLabel(byte sideCode) {
        return switch (sideCode) {
            case AcceptOrderCommand.SIDE_BUY -> "buy";
            case AcceptOrderCommand.SIDE_SELL -> "sell";
            default -> "unknown";
        };
    }

    private static String tifLabel(byte tifCode) {
        return switch (tifCode) {
            case AcceptOrderCommand.TIF_DAY -> "day";
            case AcceptOrderCommand.TIF_IOC -> "ioc";
            case AcceptOrderCommand.TIF_FOK -> "fok";
            case AcceptOrderCommand.TIF_GTC -> "gtc";
            default -> "unknown";
        };
    }

    private static Timer.Builder baseTimer(String name) {
        return Timer.builder(name)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MIN_EXPECTED_MS))
                .maximumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MAX_EXPECTED_MS));
    }
}
