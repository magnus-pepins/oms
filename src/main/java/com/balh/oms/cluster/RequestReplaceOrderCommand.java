package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster command: user-initiated modify (qty and/or limit price) routed to the broker via FIX
 * 35=G OrderCancelReplaceRequest. Issued by the OMS replace HTTP endpoint
 * ({@code POST /internal/v1/orders/{id}/replace}).
 *
 * <p>The cluster service handles this idempotently:
 * <ul>
 *   <li><strong>Unknown {@code orderId}</strong>: silent no-op.</li>
 *   <li><strong>Already-terminal order</strong>: silent no-op (caller surfaces 409).</li>
 *   <li><strong>Live order</strong>: emit one {@link OrderReplaceRequestedEvent} on the side
 *       publication. <strong>Do not</strong> mutate {@link AdmittedOrder#quantityScaled()} /
 *       {@code limitPriceScaledOrZero()} — the order keeps its current values until the broker's
 *       ER (ET=5 REPLACED) lands and {@link ApplyExecutionReportCommand} with
 *       {@code execTypeCode=EXEC_TYPE_REPLACE} applies the broker's authoritative replacement.</li>
 *   <li><strong>Duplicate request</strong> (same {@code clientRequestKey}): silent no-op.</li>
 *   <li><strong>Cumulative-fill overflow</strong>: if the new qty is less than the already-filled
 *       cumQty, the cluster silently drops the command (the order has already been over-replaced
 *       beyond what the broker can honor; the caller surfaces 409). Mirrors the existing
 *       fill-overflow no-op in {@link OmsAdmissionClusteredService#applyExecutionReport}.</li>
 * </ul>
 *
 * <p>{@code newQuantityScaled} uses the same {@link AcceptOrderCommand#QUANTITY_SCALE} fixed-point
 * convention as the original order. {@code newLimitPriceScaledOrZero} uses
 * {@link AcceptOrderCommand#PRICE_SCALE}, with {@code 0L} meaning "no price change requested" —
 * the egress preserves the original limit price when building 35=G in that case.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   newQuantityScaled
 *   offset 40  long   newLimitPriceScaledOrZero   (0 means "keep existing price")
 *   offset 48  long   requestedAtNanos
 *   offset 56  string clientRequestKey
 *   offset N   string reason
 * </pre>
 */
public record RequestReplaceOrderCommand(
        long correlationId,
        UUID orderId,
        long newQuantityScaled,
        long newLimitPriceScaledOrZero,
        long requestedAtNanos,
        String clientRequestKey,
        String reason) {

    public RequestReplaceOrderCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(clientRequestKey, "clientRequestKey");
        Objects.requireNonNull(reason, "reason");
        if (newQuantityScaled <= 0L) {
            throw new IllegalArgumentException("newQuantityScaled must be > 0: " + newQuantityScaled);
        }
        if (newLimitPriceScaledOrZero < 0L) {
            throw new IllegalArgumentException(
                    "newLimitPriceScaledOrZero must be >= 0: " + newLimitPriceScaledOrZero);
        }
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_REQUEST_REPLACE_ORDER);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, newQuantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, newLimitPriceScaledOrZero);
        p += Long.BYTES;
        buffer.putLong(p, requestedAtNanos);
        p += Long.BYTES;
        p = writeString(buffer, p, clientRequestKey);
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "RequestReplaceOrderCommand encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static RequestReplaceOrderCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_REQUEST_REPLACE_ORDER) {
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
        long newQuantityScaled = buffer.getLong(p);
        p += Long.BYTES;
        long newLimitPriceScaledOrZero = buffer.getLong(p);
        p += Long.BYTES;
        long requestedAtNanos = buffer.getLong(p);
        p += Long.BYTES;
        String clientRequestKey = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String reason = readString(buffer, p);

        return new RequestReplaceOrderCommand(
                correlationId,
                new UUID(msb, lsb),
                newQuantityScaled,
                newLimitPriceScaledOrZero,
                requestedAtNanos,
                clientRequestKey,
                reason);
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
