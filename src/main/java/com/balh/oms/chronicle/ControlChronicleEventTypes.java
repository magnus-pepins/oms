package com.balh.oms.chronicle;

/**
 * {@link com.balh.oms.proto.control.v1.ControlPendingEvent#getType()} values on the control Chronicle.
 */
public final class ControlChronicleEventTypes {

    private ControlChronicleEventTypes() {}

    public static final String ORDER_ACCEPTED = "OrderAccepted";

    /**
     * Chronicle-only follow-up after {@link #ORDER_ACCEPTED} CAS: same {@code telemetry_hops} ordering as the
     * accepted message, plus {@link PipelineTelemetryStages#CONTROL_TAIL_APPLY}. Does not use {@code control_outbox}
     * and does not pass through {@link com.balh.oms.tailer.ControlTailer}.
     */
    public static final String CONTROL_PIPELINE_TELEMETRY = "ControlPipelineTelemetry";
}
