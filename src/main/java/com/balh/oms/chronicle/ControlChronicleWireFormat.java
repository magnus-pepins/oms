package com.balh.oms.chronicle;

/**
 * Wire constants for Chronicle excerpts.
 *
 * <p>Slice 3f (oms-aeron-cluster-substrate plan) removed the {@code OUTBOX_*} JSONB-wrapper
 * constants together with the {@code control_outbox} table; only the Chronicle excerpt prefix
 * survives, and slice 3g removes that too along with the rest of the chronicle module.
 */
public final class ControlChronicleWireFormat {

    private ControlChronicleWireFormat() {}

    /**
     * Magic bytes prepended to {@link com.balh.oms.proto.control.v1.ControlPendingEvent} protobuf body on Chronicle
     * ({@code OrderAccepted}, {@code ControlPipelineTelemetry}, …).
     */
    public static final byte[] CHRONICLE_PROTO_PREFIX = new byte[] {'O', 'M', 'S', 0x01};

    public static final int CHRONICLE_PROTO_PREFIX_LENGTH = CHRONICLE_PROTO_PREFIX.length;

    public static boolean chronicleExcerptStartsWithProtoPrefix(byte[] excerpt) {
        return startsWithPrefix(excerpt, CHRONICLE_PROTO_PREFIX);
    }

    private static boolean startsWithPrefix(byte[] excerpt, byte[] prefix) {
        if (excerpt == null || excerpt.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (excerpt[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
