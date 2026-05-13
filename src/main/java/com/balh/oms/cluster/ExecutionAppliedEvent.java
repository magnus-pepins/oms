package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Projection event emitted by {@link OmsAdmissionClusteredService} on every <strong>fresh</strong>
 * {@link ApplyExecutionReportCommand} (Phase 3 of
 * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>Travels the side projection publication ({@link OmsClusterWireFormat#EVENTS_CHANNEL} /
 * {@link OmsClusterWireFormat#EVENTS_STREAM_ID}) — the same channel as
 * {@link OrderAdmittedEvent} — and is recorded by Aeron Archive on each cluster member. Slice 3e
 * folds the projector consumer that turns this event into {@code executions} +
 * {@code orders} (status / version / cum_filled_quantity update) + {@code control_decisions} writes
 * in one TX, replacing {@code ExecutionReportApplier}.
 *
 * <p>Idempotent re-hits inside the cluster's state machine
 * ({@code (orderId, venueExecRef)} already in the in-memory dedupe set) do <strong>not</strong>
 * emit a second event; the first emission's recording is the authoritative projection signal.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 *
 * <pre>
 *   offset 16  long  orderIdMsb
 *   offset 24  long  orderIdLsb
 *   offset 32  long  newCumQtyScaled            (post-apply cumulative quantity, 1e9 fixed-point)
 *   offset 40  long  lastQtyScaled              (1e9 fixed-point; 0 for non-trade)
 *   offset 48  long  lastPxScaled               (1e6 fixed-point; 0 for non-trade or no price)
 *   offset 56  long  venueTsNanos               (echoed from the command)
 *   offset 64  long  appliedAtMillis            (cluster timestamp at apply, epoch millis;
 *                                                  Aeron Cluster {@code ConsensusModule.Context.timeUnit()}
 *                                                  default {@code TimeUnit.MILLISECONDS})
 *   offset 72  int   newVersion                 (post-apply order.version)
 *   offset 76  byte  execTypeCode               (echoed; 0 == TRADE, 1 == CANCEL, 2 == VENUE_REJECT)
 *   offset 77  byte  newStatusCode              (post-apply order.status; OrderStatus ordinal)
 *   offset 78  byte  rejectCodeOrZero           (only meaningful when execTypeCode == VENUE_REJECT)
 *   offset 79  byte  reserved (must be 0)
 *   offset 80  string accountId
 *   offset N   string venueId
 *   offset N   string venueExecRef
 *   offset N   string rawEnvelopeJson
 * </pre>
 *
 * <p>{@code accountId} is included so the projector can write the {@code executions} row without a
 * second Postgres lookup ({@code executions.account_id} is set from the order's account at apply
 * time). {@code newCumQtyScaled} echoes the cluster's post-apply cumulative quantity so the
 * projector's {@code orders} CAS does not need to re-derive it from the {@code executions} log.
 */
public record ExecutionAppliedEvent(
        UUID orderId,
        long newCumQtyScaled,
        long lastQtyScaled,
        long lastPxScaled,
        long venueTsNanos,
        long appliedAtMillis,
        int newVersion,
        byte execTypeCode,
        byte newStatusCode,
        byte rejectCodeOrZero,
        String accountId,
        String venueId,
        String venueExecRef,
        String rawEnvelopeJson) {

    public ExecutionAppliedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(venueId, "venueId");
        Objects.requireNonNull(venueExecRef, "venueExecRef");
        Objects.requireNonNull(rawEnvelopeJson, "rawEnvelopeJson");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(
                p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET,
                OmsClusterWireFormat.TYPE_ID_EXECUTION_APPLIED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, 0L);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, newCumQtyScaled);
        p += Long.BYTES;
        buffer.putLong(p, lastQtyScaled);
        p += Long.BYTES;
        buffer.putLong(p, lastPxScaled);
        p += Long.BYTES;
        buffer.putLong(p, venueTsNanos);
        p += Long.BYTES;
        buffer.putLong(p, appliedAtMillis);
        p += Long.BYTES;
        buffer.putInt(p, newVersion);
        p += Integer.BYTES;
        buffer.putByte(p++, execTypeCode);
        buffer.putByte(p++, newStatusCode);
        buffer.putByte(p++, rejectCodeOrZero);
        buffer.putByte(p++, (byte) 0);

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, venueId);
        p = writeString(buffer, p, venueExecRef);
        p = writeString(buffer, p, rawEnvelopeJson);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "ExecutionAppliedEvent encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static ExecutionAppliedEvent decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_EXECUTION_APPLIED) {
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
        long newCumQtyScaled = buffer.getLong(p);
        p += Long.BYTES;
        long lastQtyScaled = buffer.getLong(p);
        p += Long.BYTES;
        long lastPxScaled = buffer.getLong(p);
        p += Long.BYTES;
        long venueTsNanos = buffer.getLong(p);
        p += Long.BYTES;
        long appliedAtMillis = buffer.getLong(p);
        p += Long.BYTES;
        int newVersion = buffer.getInt(p);
        p += Integer.BYTES;
        byte execTypeCode = buffer.getByte(p++);
        byte newStatusCode = buffer.getByte(p++);
        byte rejectCodeOrZero = buffer.getByte(p++);
        p++; // reserved

        String accountId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String venueId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String venueExecRef = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String rawEnvelopeJson = readString(buffer, p);

        return new ExecutionAppliedEvent(
                new UUID(msb, lsb),
                newCumQtyScaled,
                lastQtyScaled,
                lastPxScaled,
                venueTsNanos,
                appliedAtMillis,
                newVersion,
                execTypeCode,
                newStatusCode,
                rejectCodeOrZero,
                accountId,
                venueId,
                venueExecRef,
                rawEnvelopeJson);
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

    /**
     * Number of bytes a string field occupies on the wire, read directly from the 4-byte length
     * prefix in {@code buffer} at {@code offset}. See {@code AcceptOrderCommand#stringByteLenAt}
     * for the slice-4f rationale (eliminates redundant {@code byte[]} allocations on decode).
     */
    private static int stringByteLenAt(DirectBuffer buffer, int offset) {
        return Integer.BYTES + buffer.getInt(offset);
    }
}
