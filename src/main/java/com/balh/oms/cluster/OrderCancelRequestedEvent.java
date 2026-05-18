package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Projection event emitted by {@link OmsAdmissionClusteredService} after admitting a
 * {@link RequestCancelOrderCommand}. Two consumers:
 *
 * <ol>
 *   <li><strong>oms-fix-egress</strong>: builds FIX 35=F OrderCancelRequest from this event and
 *       sends it to the broker. The egress's {@code OmsFixEgressService} dispatches on the event
 *       type-id ({@link OmsClusterWireFormat#TYPE_ID_ORDER_CANCEL_REQUESTED}) in its replay loop,
 *       same shape as the existing OrderAdmittedEvent → 35=D path.</li>
 *   <li><strong>oms-postgres-projector</strong> (optional): writes a {@code domain_event_outbox}
 *       row with envelope {@code OrderCancelRequested} so the trading-desk + customer-frontend
 *       can render a transient "cancel requested" badge while the broker round-trips.</li>
 * </ol>
 *
 * <p>The order's status is <strong>not</strong> mutated by this event — the order stays
 * WORKING / PARTIALLY_FILLED until the broker's ER (ET=4 CANCELED via
 * {@link ApplyExecutionReportCommand#EXEC_TYPE_CANCEL} or 35=9 via
 * {@link ApplyExecutionReportCommand#EXEC_TYPE_CANCEL_REJECT}) lands.
 *
 * <p>The {@code clientRequestKey} is carried through so the egress can dedupe on it (a re-emitted
 * event during cluster log replay reuses the same key, and the egress's cursor + Postgres
 * idempotency table prevents a second 35=F).
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   originalQuantityScaled   (order.quantityScaled at request time)
 *   offset 40  long   cumQtyScaled             (already-filled qty at request time)
 *   offset 48  long   requestedAtMillis        (cluster timestamp; epoch millis)
 *   offset 56  int    shardId
 *   offset 60  byte   sideCode                 (mirrors AcceptOrderCommand.SIDE_*)
 *   offset 61  byte   reserved (must be 0)
 *   offset 62  byte   reserved (must be 0)
 *   offset 63  byte   reserved (must be 0)
 *   offset 64  string accountId
 *   offset N   string instrumentSymbol
 *   offset N   string clientRequestKey
 *   offset N   string reason
 * </pre>
 *
 * <p>{@code sideCode} + {@code originalQuantityScaled} + {@code cumQtyScaled} are carried so
 * oms-fix-egress can build a complete FIX 35=F (which requires {@code Side(54)} and
 * {@code OrderQty(38)}) without consulting Postgres or the cluster — preserving the
 * Aeron→FIX hot send path.
 */
public record OrderCancelRequestedEvent(
        UUID orderId,
        long originalQuantityScaled,
        long cumQtyScaled,
        long requestedAtMillis,
        int shardId,
        byte sideCode,
        String accountId,
        String instrumentSymbol,
        String clientRequestKey,
        String reason) {

    public OrderCancelRequestedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
        Objects.requireNonNull(clientRequestKey, "clientRequestKey");
        Objects.requireNonNull(reason, "reason");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, 0L);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, originalQuantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, cumQtyScaled);
        p += Long.BYTES;
        buffer.putLong(p, requestedAtMillis);
        p += Long.BYTES;
        buffer.putInt(p, shardId);
        p += Integer.BYTES;
        buffer.putByte(p++, sideCode);
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, instrumentSymbol);
        p = writeString(buffer, p, clientRequestKey);
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "OrderCancelRequestedEvent encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static OrderCancelRequestedEvent decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED) {
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
        long cumQtyScaled = buffer.getLong(p);
        p += Long.BYTES;
        long requestedAtMillis = buffer.getLong(p);
        p += Long.BYTES;
        int shardId = buffer.getInt(p);
        p += Integer.BYTES;
        byte sideCode = buffer.getByte(p++);
        p += 3; // reserved bytes

        String accountId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String instrumentSymbol = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String clientRequestKey = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String reason = readString(buffer, p);

        return new OrderCancelRequestedEvent(
                new UUID(msb, lsb),
                originalQuantityScaled,
                cumQtyScaled,
                requestedAtMillis,
                shardId,
                sideCode,
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
