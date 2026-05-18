package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Projection event emitted by {@link OmsAdmissionClusteredService} after admitting a
 * {@link RequestReplaceOrderCommand}. Mirror of {@link OrderCancelRequestedEvent} for the modify
 * path: oms-fix-egress consumes this and sends 35=G OrderCancelReplaceRequest to the broker.
 *
 * <p>Carries the original order's current side / quantity / limit price / TIF / instrument so the
 * egress can build a complete 35=G without re-reading any state — the cluster is the source of
 * truth and the events recording is replay-deterministic. {@code newLimitPriceScaledOrZero == 0}
 * means "keep existing limit price"; the egress copies {@code originalLimitPriceScaledOrZero}
 * into the outbound 35=G Price field in that case.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   originalQuantityScaled
 *   offset 40  long   originalLimitPriceScaledOrZero
 *   offset 48  long   newQuantityScaled
 *   offset 56  long   newLimitPriceScaledOrZero
 *   offset 64  long   requestedAtMillis
 *   offset 72  int    shardId
 *   offset 76  byte   sideCode                 (0=BUY, 1=SELL — mirrors AcceptOrderCommand)
 *   offset 77  byte   timeInForceCode          (mirrors AcceptOrderCommand)
 *   offset 78  byte   reserved (must be 0)
 *   offset 79  byte   reserved (must be 0)
 *   offset 80  string accountId
 *   offset N   string instrumentSymbol
 *   offset N   string clientRequestKey
 *   offset N   string reason
 * </pre>
 */
public record OrderReplaceRequestedEvent(
        UUID orderId,
        long originalQuantityScaled,
        long originalLimitPriceScaledOrZero,
        long newQuantityScaled,
        long newLimitPriceScaledOrZero,
        long requestedAtMillis,
        int shardId,
        byte sideCode,
        byte timeInForceCode,
        String accountId,
        String instrumentSymbol,
        String clientRequestKey,
        String reason) {

    public OrderReplaceRequestedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
        Objects.requireNonNull(clientRequestKey, "clientRequestKey");
        Objects.requireNonNull(reason, "reason");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, 0L);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, originalQuantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, originalLimitPriceScaledOrZero);
        p += Long.BYTES;
        buffer.putLong(p, newQuantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, newLimitPriceScaledOrZero);
        p += Long.BYTES;
        buffer.putLong(p, requestedAtMillis);
        p += Long.BYTES;
        buffer.putInt(p, shardId);
        p += Integer.BYTES;
        buffer.putByte(p++, sideCode);
        buffer.putByte(p++, timeInForceCode);
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, instrumentSymbol);
        p = writeString(buffer, p, clientRequestKey);
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "OrderReplaceRequestedEvent encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static OrderReplaceRequestedEvent decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        int schema = buffer.getInt(offset + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET);
        if (schema != OmsClusterWireFormat.SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schema version " + schema);
        }

        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        long originalQuantityScaled = buffer.getLong(p);
        p += Long.BYTES;
        long originalLimitPriceScaledOrZero = buffer.getLong(p);
        p += Long.BYTES;
        long newQuantityScaled = buffer.getLong(p);
        p += Long.BYTES;
        long newLimitPriceScaledOrZero = buffer.getLong(p);
        p += Long.BYTES;
        long requestedAtMillis = buffer.getLong(p);
        p += Long.BYTES;
        int shardId = buffer.getInt(p);
        p += Integer.BYTES;
        byte sideCode = buffer.getByte(p++);
        byte timeInForceCode = buffer.getByte(p++);
        p += 2; // reserved bytes

        String accountId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String instrumentSymbol = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String clientRequestKey = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String reason = readString(buffer, p);

        return new OrderReplaceRequestedEvent(
                new UUID(msb, lsb),
                originalQuantityScaled,
                originalLimitPriceScaledOrZero,
                newQuantityScaled,
                newLimitPriceScaledOrZero,
                requestedAtMillis,
                shardId,
                sideCode,
                timeInForceCode,
                accountId,
                instrumentSymbol,
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
