package com.balh.oms.observability.metrics;

/**
 * Micrometer {@link io.micrometer.core.instrument.Timer} names for the order pipeline.
 * Prometheus export maps dots to underscores and appends {@code _seconds_*} for timers.
 *
 * <p>Phase 4 slice 4i housekeeping: the {@code oms.pipeline.control.apply},
 * {@code oms.pipeline.fix.outbound_nos}, and {@code oms.pipeline.ingress_to_fix_nos} constants
 * lived here as a Phase-1 monolith breakdown. Their producers were deleted across slices 3b-2,
 * 3f, and 3g; the constants had no callers (verified at slice 4i landing) so they were removed
 * along with the orphan {@code IngressToFixNosLatencyRecorder}. Per-JVM substitutes:
 * {@code oms.cluster.client.commit_round_trip} (slice 4c, ingress-replica),
 * {@code oms.fix_egress.lag_seconds} (slice 4d, fix-egress JVM).
 */
public final class OmsPipelineMeterNames {

    private OmsPipelineMeterNames() {}

    /**
     * Internal accept: Postgres transaction for the ingress JVM until commit (domain_event_outbox +
     * optional ledger_inflight_outbox); orders writes moved to OmsPostgresProjector in slice 2c and
     * control_outbox was deleted in slice 3f.
     * Tag {@code outcome}: {@code created}, {@code duplicate}, {@code error}.
     */
    public static final String INGRESS_ACCEPT = "oms.pipeline.ingress.accept";
}
