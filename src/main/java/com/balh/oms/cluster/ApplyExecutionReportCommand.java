package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster command: apply a venue execution report (fill / partial / cancel / venue-reject) to an
 * order the cluster has already admitted.
 *
 * <p>This is the cluster-side analog of {@code ExecutionReportApplier} in the legacy code. The
 * deterministic state machine inside {@link OmsAdmissionClusteredService} walks the order state
 * machine and emits an {@link ExecutionAppliedEvent} on the events stream; the projector (slice 3e)
 * then consumes that event and writes {@code executions} + {@code orders} + {@code control_decisions}
 * in one TX. <strong>No Postgres lookup, no external I/O is permitted on the apply path.</strong>
 *
 * <p>Time and identifiers are <strong>supplied by the cluster client</strong> (the
 * {@code oms-fix-egress} JVM in slice 3d) so the apply is deterministic across leader, follower, and
 * replay.
 *
 * <h2>Idempotency keys</h2>
 *
 * <ul>
 *   <li><strong>{@code (orderId, venueExecRef)}</strong> — the natural FIX-level dedupe key
 *       (mirrors the existing {@code executions} unique constraint on
 *       {@code (account_id, venue_exec_ref)}). Slice 3c enforces this inside the clustered service:
 *       if the same {@code (orderId, venueExecRef)} arrives twice, the state machine emits no second
 *       {@link ExecutionAppliedEvent} and the projector therefore writes no second {@code executions}
 *       row.</li>
 *   <li><strong>{@code (senderCompId, msgSeqNum)}</strong> — wire-level dedupe key used by slice 3d
 *       when {@code oms-fix-egress} retries an offer to the cluster ingress. Reserved on the wire
 *       today (carried as fields and decoded into the record) but the state-machine guard for it
 *       lands in slice 3d alongside the inbound ER → cluster path. Carrying the field in slice 3c
 *       avoids a wire-format break when slice 3d turns the guard on.</li>
 * </ul>
 *
 * <h2>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header)</h2>
 *
 * <pre>
 *   offset 16  long  orderIdMsb
 *   offset 24  long  orderIdLsb
 *   offset 32  long  lastQtyScaled              (1e9 fixed-point; 0 for non-trade)
 *   offset 40  long  lastPxScaled               (1e6 fixed-point; 0 for non-trade or no price)
 *   offset 48  long  venueTsNanos               (epoch nanos, venue wall-time)
 *   offset 56  int   msgSeqNum                  (slice 3d FIX ER seq; 0 in slice 3c tests)
 *   offset 60  byte  execTypeCode               (0 == TRADE, 1 == CANCEL, 2 == VENUE_REJECT)
 *   offset 61  byte  rejectCodeOrZero           (only meaningful when execTypeCode == VENUE_REJECT)
 *   offset 62  byte  reserved (must be 0)
 *   offset 63  byte  reserved (must be 0)
 *   offset 64  string venueId
 *   offset N   string venueExecRef              (FIX ExecID; idempotency key with orderId)
 *   offset N   string senderCompId              (slice 3d FIX sender; "" in slice 3c tests)
 *   offset N   string rawEnvelopeJson           (executions.raw_envelope_json projection input)
 * </pre>
 *
 * <p>Strings use the same length-prefixed UTF-8 encoding as {@link AcceptOrderCommand} (4-byte int
 * length followed by bytes; max {@link OmsClusterWireFormat#MAX_STRING_BYTES} bytes). Maximum total
 * encoded size is bounded by {@link OmsClusterWireFormat#MAX_COMMAND_BYTES}.
 */
public record ApplyExecutionReportCommand(
        long correlationId,
        UUID orderId,
        long lastQtyScaled,
        long lastPxScaled,
        long venueTsNanos,
        int msgSeqNum,
        byte execTypeCode,
        byte rejectCodeOrZero,
        String venueId,
        String venueExecRef,
        String senderCompId,
        String rawEnvelopeJson) {

    /** Execution-type wire codes. Stable; never reuse a number for a different type. */
    public static final byte EXEC_TYPE_TRADE = 0;

    public static final byte EXEC_TYPE_CANCEL = 1;
    public static final byte EXEC_TYPE_VENUE_REJECT = 2;

    /**
     * Wed-demo addition. ER ET=5 (REPLACED) from the broker in response to a 35=G we sent. The
     * cluster's apply path interprets {@code lastQtyScaled} as the <strong>new total OrderQty</strong>
     * (broker's authoritative replacement quantity, not the trade quantity — there is no fill on a
     * pure replace) and {@code lastPxScaled} as the <strong>new limit price</strong>. The order
     * record's {@code quantityScaled} / {@code limitPriceScaledOrZero} are overwritten and version
     * is bumped; status / cumQty stay unchanged.
     */
    public static final byte EXEC_TYPE_REPLACE = 3;

    /**
     * Wed-demo addition. 35=9 OrderCancelReject from the broker in response to one of our 35=F
     * cancels. The cluster's apply path emits an {@link OrderCancelRejectedEvent} for the projector
     * + UI toast and otherwise leaves the order untouched (status, cumQty, version all unchanged).
     * Carried inside {@link ApplyExecutionReportCommand} so the existing FIX inbound sink can
     * submit one command-shape for every ER-or-OCR; the cluster decodes the byte and dispatches.
     */
    public static final byte EXEC_TYPE_CANCEL_REJECT = 4;

    /** Mirror of {@link #EXEC_TYPE_CANCEL_REJECT} for a 35=9 against a 35=G replace. */
    public static final byte EXEC_TYPE_REPLACE_REJECT = 5;

    /**
     * Venue acceptance of a previously-admitted order (FIX {@code 35=8} {@code ExecType=New (150=0)} /
     * {@code OrdStatus=New (39=0)}, or an internal-venue gRPC rest ack with no fill). Promotes the
     * order from {@code PENDING_NEW} (admitted at OMS, routed, awaiting venue) to {@code WORKING}
     * (confirmed live at the venue). Idempotent: a second venue-new for an order already past
     * {@code PENDING_NEW} (WORKING / PARTIALLY_FILLED / FILLED / terminal) is a no-op. Carries no
     * fill quantity or price ({@code lastQtyScaled == 0}, {@code lastPxScaled == 0}).
     */
    public static final byte EXEC_TYPE_VENUE_NEW = 6;

    public ApplyExecutionReportCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(venueId, "venueId");
        Objects.requireNonNull(venueExecRef, "venueExecRef");
        Objects.requireNonNull(senderCompId, "senderCompId");
        Objects.requireNonNull(rawEnvelopeJson, "rawEnvelopeJson");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(
                p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET,
                OmsClusterWireFormat.TYPE_ID_APPLY_EXECUTION_REPORT);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, lastQtyScaled);
        p += Long.BYTES;
        buffer.putLong(p, lastPxScaled);
        p += Long.BYTES;
        buffer.putLong(p, venueTsNanos);
        p += Long.BYTES;
        buffer.putInt(p, msgSeqNum);
        p += Integer.BYTES;
        buffer.putByte(p++, execTypeCode);
        buffer.putByte(p++, rejectCodeOrZero);
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);

        p = writeString(buffer, p, venueId);
        p = writeString(buffer, p, venueExecRef);
        p = writeString(buffer, p, senderCompId);
        p = writeString(buffer, p, rawEnvelopeJson);

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "ApplyExecutionReportCommand encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static ApplyExecutionReportCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_APPLY_EXECUTION_REPORT) {
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
        long lastQtyScaled = buffer.getLong(p);
        p += Long.BYTES;
        long lastPxScaled = buffer.getLong(p);
        p += Long.BYTES;
        long venueTsNanos = buffer.getLong(p);
        p += Long.BYTES;
        int msgSeqNum = buffer.getInt(p);
        p += Integer.BYTES;
        byte execTypeCode = buffer.getByte(p++);
        byte rejectCodeOrZero = buffer.getByte(p++);
        p += 2; // reserved bytes

        String venueId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String venueExecRef = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String senderCompId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String rawEnvelopeJson = readString(buffer, p);

        return new ApplyExecutionReportCommand(
                correlationId,
                new UUID(msb, lsb),
                lastQtyScaled,
                lastPxScaled,
                venueTsNanos,
                msgSeqNum,
                execTypeCode,
                rejectCodeOrZero,
                venueId,
                venueExecRef,
                senderCompId,
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
