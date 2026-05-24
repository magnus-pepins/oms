package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * FIX-in ingress identity carried on the cluster wire (append-only tail on
 * {@link AcceptOrderCommand} / {@link OrderAdmittedEvent}) so replay and recovery
 * can rebuild client {@code ClOrdID} mappings without relying on a side table alone.
 */
public record FixInIngressMetadata(UUID fixSessionId, String externalClOrdId, String fixAccountTagOrEmpty) {

    public static final byte INGRESS_TYPE_NONE = 0;
    public static final byte INGRESS_TYPE_FIX_IN = 1;

    public FixInIngressMetadata {
        Objects.requireNonNull(fixSessionId, "fixSessionId");
        Objects.requireNonNull(externalClOrdId, "externalClOrdId");
        Objects.requireNonNull(fixAccountTagOrEmpty, "fixAccountTagOrEmpty");
        if (externalClOrdId.isBlank()) {
            throw new IllegalArgumentException("externalClOrdId must not be blank");
        }
    }

    public static int writeFixInTail(MutableDirectBuffer buffer, int offset, FixInIngressMetadata metadata) {
        int p = offset;
        buffer.putByte(p++, INGRESS_TYPE_FIX_IN);
        p = writeString(buffer, p, metadata.fixSessionId().toString());
        p = writeString(buffer, p, metadata.externalClOrdId());
        p = writeString(buffer, p, metadata.fixAccountTagOrEmpty());
        return p - offset;
    }

    /**
     * Reads optional FIX-in tail when {@code offset < endExclusive}. Returns {@code null} when no tail
     * bytes remain (REST / legacy cluster log entries).
     */
    public static FixInIngressMetadata readFixInTailIfPresent(DirectBuffer buffer, int offset, int endExclusive) {
        if (offset >= endExclusive) {
            return null;
        }
        byte ingressType = buffer.getByte(offset);
        if (ingressType == INGRESS_TYPE_NONE) {
            return null;
        }
        if (ingressType != INGRESS_TYPE_FIX_IN) {
            throw new IllegalArgumentException("unsupported ingress type byte on wire: " + ingressType);
        }
        int p = offset + 1;
        String sessionIdRaw = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String externalClOrdId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String fixAccountTag = readString(buffer, p);
        return new FixInIngressMetadata(UUID.fromString(sessionIdRaw.trim()), externalClOrdId, fixAccountTag);
    }

    private static int writeString(MutableDirectBuffer buffer, int offset, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "string length " + bytes.length + " exceeds MAX_STRING_BYTES="
                            + OmsClusterWireFormat.MAX_STRING_BYTES);
        }
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + Integer.BYTES, bytes);
        return offset + Integer.BYTES + bytes.length;
    }

    private static String readString(DirectBuffer buffer, int offset) {
        int len = buffer.getInt(offset);
        if (len < 0 || len > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "string length " + len + " out of bounds [0, "
                            + OmsClusterWireFormat.MAX_STRING_BYTES + "]");
        }
        byte[] bytes = new byte[len];
        buffer.getBytes(offset + Integer.BYTES, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int stringByteLenAt(DirectBuffer buffer, int offset) {
        return Integer.BYTES + buffer.getInt(offset);
    }
}
