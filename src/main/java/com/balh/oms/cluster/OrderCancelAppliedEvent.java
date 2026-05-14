package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Projection event emitted by {@link OmsAdmissionClusteredService} after applying a
 * {@link CancelOrderCommand}. Phase 4 slice 4p artifact of the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>Distinct from {@link ExecutionAppliedEvent} with {@code execTypeCode=EXEC_TYPE_CANCEL}: that
 * event represents a venue-side cancel and carries {@code venueId} / {@code venueExecRef} for the
 * downstream {@code executions} insert. {@code OrderCancelAppliedEvent} represents an
 * OMS-initiated cancel that never touched a venue, so the projector branch on this event:
 * <ul>
 *   <li>does <strong>not</strong> insert an {@code executions} row;</li>
 *   <li>CAS-mutates {@code orders.status} to CANCELLED with {@code expectedVersion = newVersion - 1};</li>
 *   <li>inserts a {@code domain_event_outbox} row carrying an {@code OrderCancelled} envelope with
 *       empty {@code venueId} / {@code venueExecRef} (the existing envelope shape; downstream BFF
 *       fanout already tolerates empty venue fields, used for the same shape on tests).</li>
 * </ul>
 *
 * <p>Idempotent re-delivery (cluster log replay) is handled by the projector's CAS: a re-applied
 * cancel for an already-CANCELLED order at version {@code newVersion} sees the row at
 * {@code newVersion}, the CAS expected-version misses, and the projector silently advances its
 * cursor.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   cancelledAtMillis    (cluster timestamp; epoch millis per
 *                                            {@code ConsensusModule.Context.timeUnit()} default)
 *   offset 40  int    newVersion           (post-mutation order version)
 *   offset 44  int    shardId
 *   offset 48  string accountId
 *   offset N   string accountIdHash
 *   offset N   string instrumentSymbol
 *   offset N   string reason               (compensator-supplied; empty allowed)
 * </pre>
 *
 * <p>The header's {@code correlationId} field is set to {@code 0L} on the wire because projectors
 * consume by log position, not correlation; the originating compensator's correlation id stays
 * recorded only in {@code ledger_inflight_outbox.cancel_correlation_id} for ops debugging.
 */
public record OrderCancelAppliedEvent(
        UUID orderId,
        long cancelledAtMillis,
        int newVersion,
        int shardId,
        String accountId,
        String accountIdHash,
        String instrumentSymbol,
        String reason) {

    public OrderCancelAppliedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(accountIdHash, "accountIdHash");
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
        Objects.requireNonNull(reason, "reason");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_APPLIED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, 0L);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, cancelledAtMillis);
        p += Long.BYTES;
        buffer.putInt(p, newVersion);
        p += Integer.BYTES;
        buffer.putInt(p, shardId);
        p += Integer.BYTES;

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, accountIdHash);
        p = writeString(buffer, p, instrumentSymbol);
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "OrderCancelAppliedEvent encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static OrderCancelAppliedEvent decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_APPLIED) {
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
        long cancelledAtMillis = buffer.getLong(p);
        p += Long.BYTES;
        int newVersion = buffer.getInt(p);
        p += Integer.BYTES;
        int shardId = buffer.getInt(p);
        p += Integer.BYTES;

        String accountId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String accountIdHash = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String instrumentSymbol = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String reason = readString(buffer, p);

        return new OrderCancelAppliedEvent(
                new UUID(msb, lsb),
                cancelledAtMillis,
                newVersion,
                shardId,
                accountId,
                accountIdHash,
                instrumentSymbol,
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
