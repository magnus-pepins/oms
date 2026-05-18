package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster command: user-initiated cancel routed to the broker via FIX 35=F. Issued by the OMS
 * cancel HTTP endpoint ({@code POST /internal/v1/orders/{id}/cancel}) — distinct from
 * {@link CancelOrderCommand} which is the internal inflight-failure compensator path that never
 * touches a venue.
 *
 * <p>The cluster service handles this idempotently:
 * <ul>
 *   <li><strong>Unknown {@code orderId}</strong>: silent no-op (HTTP layer may race a cancel
 *       against an order that hasn't admitted yet — caller will retry).</li>
 *   <li><strong>Already-terminal order</strong> ({@code FILLED} / {@code CANCELLED} /
 *       {@code REJECTED}): silent no-op. The HTTP endpoint surfaces this as 409 Conflict by
 *       reading the order's current status after submitting the command.</li>
 *   <li><strong>Live order</strong>: emit one {@link OrderCancelRequestedEvent} on the side
 *       publication. <strong>Do not</strong> mutate {@link AdmittedOrder#statusCode()} — the
 *       order stays WORKING / PARTIALLY_FILLED until the broker's ER (ET=4 CANCELED or 35=9
 *       OrderCancelReject) lands.</li>
 *   <li><strong>Duplicate request</strong> (same {@code clientRequestKey}): silent no-op via the
 *       order's existing {@code requestedCancelKeys} dedupe set; mirrors the
 *       {@code (orderId, venueExecRef)} dedupe used on the ER apply path.</li>
 * </ul>
 *
 * <p>{@code clientRequestKey} is the HTTP-layer idempotency key (typically the request's
 * {@code Idempotency-Key} header, or a synthesized {@code (orderId, attempt)} value). It exists
 * so an HTTP retry of "cancel this order" does not issue two 35=F messages to the broker.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   requestedAtNanos     (HTTP-edge wall-time, epoch nanos)
 *   offset 40  string clientRequestKey     (HTTP idempotency key; empty allowed)
 *   offset N   string reason               (operator-supplied; empty allowed)
 * </pre>
 */
public record RequestCancelOrderCommand(
        long correlationId,
        UUID orderId,
        long requestedAtNanos,
        String clientRequestKey,
        String reason) {

    public RequestCancelOrderCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(clientRequestKey, "clientRequestKey");
        Objects.requireNonNull(reason, "reason");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_REQUEST_CANCEL_ORDER);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, requestedAtNanos);
        p += Long.BYTES;
        p = writeString(buffer, p, clientRequestKey);
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "RequestCancelOrderCommand encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static RequestCancelOrderCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_REQUEST_CANCEL_ORDER) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        int schema = buffer.getInt(offset + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET);
        if (schema != OmsClusterWireFormat.SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schema version " + schema);
        }
        long correlationId = buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);

        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        long requestedAtNanos = buffer.getLong(p);
        p += Long.BYTES;
        String clientRequestKey = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String reason = readString(buffer, p);

        return new RequestCancelOrderCommand(
                correlationId, new UUID(msb, lsb), requestedAtNanos, clientRequestKey, reason);
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
