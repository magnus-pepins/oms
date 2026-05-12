package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster egress event: emitted by {@link OmsAdmissionClusteredService} when an
 * {@link AcceptOrderCommand} is rejected at admission time.
 *
 * <p>Used today only for malformed-input rejection (validation failures the
 * state machine can detect without external lookups). Risk-driven rejects move
 * here in plan Phase 2 once {@code ControlRiskEvaluator} is lifted into the
 * deterministic apply path.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long  orderIdMsb
 *   offset 24  long  orderIdLsb
 *   offset 32  int   rejectCodeOrdinal  (matches RejectCode enum ordinal)
 *   offset 36  long  rejectedAtNanos    (cluster-supplied wall time)
 *   offset 44  string reason            (length-prefixed UTF-8)
 * </pre>
 */
public record OrderRejectedEvent(
        long correlationId,
        UUID orderId,
        int rejectCodeOrdinal,
        long rejectedAtNanos,
        String reason) {

    public OrderRejectedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(reason, "reason");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_REJECTED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putInt(p, rejectCodeOrdinal);
        p += Integer.BYTES;
        buffer.putLong(p, rejectedAtNanos);
        p += Long.BYTES;

        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "reason length " + bytes.length + " exceeds MAX_STRING_BYTES");
        }
        buffer.putInt(p, bytes.length);
        p += Integer.BYTES;
        buffer.putBytes(p, bytes);
        p += bytes.length;
        return p - offset;
    }

    public static OrderRejectedEvent decode(DirectBuffer buffer, int offset) {
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_REJECTED) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        long correlationId = buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);
        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        int rejectCodeOrdinal = buffer.getInt(p);
        p += Integer.BYTES;
        long rejectedAtNanos = buffer.getLong(p);
        p += Long.BYTES;
        int len = buffer.getInt(p);
        p += Integer.BYTES;
        if (len < 0 || len > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException("reason length out of bounds: " + len);
        }
        byte[] bytes = new byte[len];
        buffer.getBytes(p, bytes);
        String reason = new String(bytes, StandardCharsets.UTF_8);

        return new OrderRejectedEvent(
                correlationId, new UUID(msb, lsb), rejectCodeOrdinal, rejectedAtNanos, reason);
    }
}
