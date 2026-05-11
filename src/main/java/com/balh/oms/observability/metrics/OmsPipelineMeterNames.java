package com.balh.oms.observability.metrics;

/**
 * Micrometer {@link io.micrometer.core.instrument.Timer} names for slice-1 order pipeline stages.
 * Prometheus export maps dots to underscores and appends {@code _seconds_*} for timers.
 */
public final class OmsPipelineMeterNames {

    private OmsPipelineMeterNames() {}

    /**
     * Internal accept: single Postgres transaction (orders + control_outbox + domain_event_outbox) until commit.
     * Tag {@code outcome}: {@code created}, {@code duplicate}, {@code error}.
     */
    public static final String INGRESS_ACCEPT = "oms.pipeline.ingress.accept";

    /**
     * Wall clock from {@code control_outbox.enqueued_at} until Chronicle append + mark appended succeeds.
     * Measures queue wait + reconciler tick (not HTTP thread).
     */
    public static final String CONTROL_OUTBOX_TO_CHRONICLE_LAG = "oms.pipeline.control.outbox_to_chronicle_lag";

    /**
     * Reconciler-local work: Chronicle append + {@code markAppended} for one outbox row.
     */
    public static final String CONTROL_CHRONICLE_APPEND = "oms.pipeline.control.chronicle_append";

    /**
     * {@link com.balh.oms.tailer.ControlTailer#apply} transaction (risk, buying power, CAS, domain outbox).
     * Tag {@code result}: {@link com.balh.oms.tailer.ControlTailer.TailResult} name or {@code exception}.
     */
    public static final String CONTROL_APPLY = "oms.pipeline.control.apply";

    /**
     * FIX worker: build NOS + {@code Session.sendToTarget} after token acquired (WORKING order only).
     * Tag {@code outcome}: {@code success}, {@code failure}.
     */
    public static final String FIX_OUTBOUND_NOS = "oms.pipeline.fix.outbound_nos";

    /**
     * End-to-end wall time from committed NEW order to successful NOS send (same window as OTel
     * {@code oms.fix.ingress_to_nos}); Micrometer form for {@code /actuator/prometheus} without enabling OTel.
     */
    public static final String PIPELINE_INGRESS_TO_FIX_NOS = "oms.pipeline.ingress_to_fix_nos";
}
