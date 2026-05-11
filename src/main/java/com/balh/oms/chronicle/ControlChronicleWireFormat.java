package com.balh.oms.chronicle;

/**
 * Wire constants for Chronicle excerpts and {@code control_outbox} JSONB wrapping.
 */
public final class ControlChronicleWireFormat {

    private ControlChronicleWireFormat() {}

    /**
     * Magic bytes prepended to {@link com.balh.oms.proto.control.v1.ControlPendingEvent} protobuf body on Chronicle
     * (distinguishes from legacy UTF-8 JSON excerpts during replay).
     */
    public static final byte[] CHRONICLE_PROTO_PREFIX = new byte[] {'O', 'M', 'S', 0x01};

    public static final int CHRONICLE_PROTO_PREFIX_LENGTH = CHRONICLE_PROTO_PREFIX.length;

    /**
     * {@code control_outbox.payload} JSON object uses this value under key {@link #OUTBOX_JSON_KEY_FORMAT} to mean
     * {@link #OUTBOX_JSON_KEY_PROTO_BASE64} holds a base64-encoded {@code ControlPendingEvent} protobuf.
     */
    public static final int OUTBOX_PAYLOAD_FORMAT_PROTO_WRAPPED = 2;

    public static final String OUTBOX_JSON_KEY_FORMAT = "v";
    public static final String OUTBOX_JSON_KEY_PROTO_BASE64 = "d";

    public static boolean chronicleExcerptStartsWithProtoPrefix(byte[] excerpt) {
        if (excerpt == null || excerpt.length < CHRONICLE_PROTO_PREFIX_LENGTH) {
            return false;
        }
        for (int i = 0; i < CHRONICLE_PROTO_PREFIX_LENGTH; i++) {
            if (excerpt[i] != CHRONICLE_PROTO_PREFIX[i]) {
                return false;
            }
        }
        return true;
    }
}
