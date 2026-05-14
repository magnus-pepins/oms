package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster command: OMS-initiated cancel of an admitted order. Issued by the
 * {@code LedgerInflightHoldFailureCompensator} (slice 4p of
 * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}) when a buying-power hold
 * recorded in {@code ledger_inflight_outbox} fails on the async path (Tier 2.5).
 *
 * <p>Distinct from {@link ApplyExecutionReportCommand} with
 * {@code execTypeCode=EXEC_TYPE_CANCEL}: this command represents a cancel that never touched a
 * venue, so {@link OmsAdmissionClusteredService}'s apply path emits an
 * {@link OrderCancelAppliedEvent} (no venue/execRef metadata) instead of an
 * {@link ExecutionAppliedEvent}; the projector therefore skips the {@code executions} insert.
 *
 * <p>The cluster service handles this idempotently:
 * <ul>
 *   <li>Unknown {@code orderId}: silent no-op (compensator may race a never-admitted order).</li>
 *   <li>Already-terminal order ({@code FILLED} / {@code CANCELLED} / {@code REJECTED}): silent
 *       no-op. This is the race window the slice 4q coalescer addresses synchronously: a venue
 *       fill that lands between the inflight-hold failure and the compensator's cancel leaves
 *       the user with an unfunded position; alerting on
 *       {@code oms_inflight_compensator_filled_before_cancel} surfaces it for ops.</li>
 *   <li>Working / partially-filled: mutate to CANCELLED, bump version, emit one
 *       {@link OrderCancelAppliedEvent} on the side publication.</li>
 * </ul>
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long   orderIdMsb
 *   offset 24  long   orderIdLsb
 *   offset 32  long   requestedAtNanos   (compensator wall-time, epoch nanos)
 *   offset 40  string reason             (compensator-supplied; empty allowed; max
 *                                          {@link OmsClusterWireFormat#MAX_STRING_BYTES})
 * </pre>
 */
public record CancelOrderCommand(
        long correlationId,
        UUID orderId,
        long requestedAtNanos,
        String reason) {

    public CancelOrderCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(reason, "reason");
    }

    /**
     * Encode this command into {@code buffer} starting at {@code offset}.
     *
     * @return the number of bytes written.
     * @throws IllegalArgumentException if {@code reason} exceeds
     *         {@link OmsClusterWireFormat#MAX_STRING_BYTES} after UTF-8 encoding, or total size
     *         would exceed {@link OmsClusterWireFormat#MAX_COMMAND_BYTES}.
     */
    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_CANCEL_ORDER);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, requestedAtNanos);
        p += Long.BYTES;
        p = writeString(buffer, p, reason);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "CancelOrderCommand encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    /**
     * Decode a {@link CancelOrderCommand} from {@code buffer} starting at {@code offset}. The
     * header is validated; the caller is responsible for dispatching by
     * {@link OmsClusterWireFormat#TYPE_ID_CANCEL_ORDER}.
     */
    public static CancelOrderCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_CANCEL_ORDER) {
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
        String reason = readString(buffer, p);

        return new CancelOrderCommand(correlationId, new UUID(msb, lsb), requestedAtNanos, reason);
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
}
