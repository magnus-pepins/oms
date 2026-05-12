package com.balh.oms.chronicle;

/**
 * {@code telemetry_hops[].stage} on {@link com.balh.oms.proto.control.v1.ControlPendingEvent}.
 */
public final class PipelineTelemetryStages {

    private PipelineTelemetryStages() {}

    /** HTTP or gRPC ingress JVM committed the outbox row (hop time = {@link PendingControlEvent#enqueuedAt()} wall). */
    public static final String INGRESS = "ingress";

    /** Outbox reconciler appended this control event to Chronicle. */
    public static final String RECONCILER_CHRONICLE_APPEND = "reconciler_chronicle_append";

    /** Chronicle tail applied CAS to Postgres; hop appears on the following {@code ControlPipelineTelemetry} excerpt. */
    public static final String CONTROL_TAIL_APPLY = "control_tail_apply";
}
