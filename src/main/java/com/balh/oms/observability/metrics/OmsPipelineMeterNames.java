package com.balh.oms.observability.metrics;

/**
 * Micrometer {@link io.micrometer.core.instrument.Timer} names for slice-1 order pipeline stages.
 * Prometheus export maps dots to underscores and appends {@code _seconds_*} for timers.
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

    /**
     * {@link com.balh.oms.tailer.OrderControlAdmission#persistAdmission} transaction (risk, buying power, CAS,
     * domain outbox), driven by the {@code OmsPostgresProjector} on cluster {@code OrderAdmittedEvent} replay.
     * Tag {@code result}: {@link com.balh.oms.tailer.OrderControlAdmission.AdmissionResult} name or {@code exception}.
     */
    public static final String CONTROL_APPLY = "oms.pipeline.control.apply";

    /**
     * FIX egress: build NOS + {@code Session.sendToTarget} after the cluster admits the order (egress JVM only).
     * Tag {@code outcome}: {@code success}, {@code failure}.
     */
    public static final String FIX_OUTBOUND_NOS = "oms.pipeline.fix.outbound_nos";

    /**
     * End-to-end wall time from committed NEW order to successful NOS send (same window as OTel
     * {@code oms.fix.ingress_to_nos}); Micrometer form for {@code /actuator/prometheus} without enabling OTel.
     */
    public static final String PIPELINE_INGRESS_TO_FIX_NOS = "oms.pipeline.ingress_to_fix_nos";
}
