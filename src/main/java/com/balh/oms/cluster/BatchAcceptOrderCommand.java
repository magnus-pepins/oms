package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Cluster command: batched form of {@link AcceptOrderCommand}.
 *
 * <p>Phase 4 Tier 2.5 phase D-6 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}.
 * Pre-D-6, every {@link AcceptOrderCommand} cost the cluster's single-leader admit thread one full
 * Aeron message round-trip (fragment delivery + consensus log append + {@code onSessionMessage}
 * dispatch + egress emit). Pop! 2026-05-14 evidence: at 57 282 rps peak, the cluster commit RTT
 * was 2.81 ms (98.3 % of total ingress accept time), and a Tomcat thread-bump experiment
 * (`SERVER_TOMCAT_THREADS_MAX=400`) regressed peak rps by −7.5 %, confirming the wall is on the
 * cluster side, not the client. D-6 amortises the per-message framing / consensus / dispatch
 * overhead across N admits per cluster log slot.
 *
 * <h3>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header)</h3>
 *
 * <pre>
 *   header (16 bytes):
 *     offset 0   int  typeId               = {@link OmsClusterWireFormat#TYPE_ID_BATCH_ACCEPT_ORDER}
 *     offset 4   int  schemaVersion        = {@link OmsClusterWireFormat#SCHEMA_VERSION}
 *     offset 8   long batchCorrelationId   (unused; inner commands carry their own correlationId)
 *   payload:
 *     offset 16  int  count                (number of inner AcceptOrderCommand bodies; 1 ≤ count ≤ MAX_COUNT)
 *     for i in [0, count):
 *       int innerLength                    (length in bytes of the inner AcceptOrderCommand wire body)
 *       byte[innerLength]                  (raw {@link AcceptOrderCommand} encoded body, with its own typeId=1 header)
 * </pre>
 *
 * <p>The inner-length prefix lets the cluster decoder iterate the batch without delegating
 * length-tracking to {@link AcceptOrderCommand#decode}, which doesn't return bytes-read.
 *
 * <h3>Determinism (per ADR 0001)</h3>
 *
 * <p>Each inner command's {@code correlationId}, {@code orderId}, and {@code clientTimestampNanos}
 * are still assigned by the calling {@link com.balh.oms.ingress.OrderIngressService} <em>before</em>
 * enqueuing into the batcher. Batching is a transport-layer optimisation; it does not move the
 * "edge" of where time / random ids are generated. The cluster's
 * {@link OmsAdmissionClusteredService#applyAcceptOrder applyAcceptOrder} path is unchanged and
 * runs once per inner command in batch arrival order.
 *
 * <h3>Idempotency / replay</h3>
 *
 * <p>The cluster log records the batched message as a single entry. On replay, the
 * {@link OmsAdmissionClusteredService} re-decodes the batch and reapplies inner commands in the
 * same order, producing identical {@link OrderAcceptedEvent}s / {@link OrderRejectedEvent}s. The
 * existing {@code (accountId, clientIdempotencyKey)} idempotency index inside the cluster service
 * still short-circuits replays of already-admitted orders; batching does not change those
 * semantics.
 */
public final class BatchAcceptOrderCommand {

    private BatchAcceptOrderCommand() {}

    /** Offset (within the batch payload, after the 16-byte header) of the {@code count} field. */
    public static final int COUNT_OFFSET = OmsClusterWireFormat.HEADER_LENGTH;

    /** Bytes occupied by the {@code count} field. */
    public static final int COUNT_LENGTH = Integer.BYTES;

    /** Bytes occupied by an inner-command length prefix. */
    public static final int INNER_LENGTH_PREFIX_BYTES = Integer.BYTES;

    /**
     * Hard upper bound on inner-command count. Bounds the cluster service decode loop and prevents
     * pathological allocations from a malformed client. The configured runtime
     * {@code maxBatchSize} (see {@code OmsConfig.Cluster.Client.AdmitBatch}) is checked against
     * this constant at startup so a typo'd config can't smuggle in a number that would oversize
     * the batch buffer.
     */
    public static final int MAX_COUNT = 256;

    /**
     * Encode the header and {@code count} field for a batch frame at {@code offset}. Caller writes
     * the inner commands themselves at {@link #firstInnerOffset(int) firstInnerOffset(offset)}
     * onwards using {@link #writeInner(MutableDirectBuffer, int, DirectBuffer, int, int)} per
     * inner element, then calls {@link #totalEncodedLength(int)} to get the wire length to pass to
     * {@code AeronCluster.offer(buffer, 0, written)}.
     *
     * @return offset of the first inner-command-length prefix (i.e. {@code firstInnerOffset(offset)}).
     */
    public static int writeHeader(MutableDirectBuffer buffer, int offset, int count) {
        if (count < 1 || count > MAX_COUNT) {
            throw new IllegalArgumentException(
                    "batch count out of bounds [1, " + MAX_COUNT + "]: " + count);
        }
        buffer.putInt(
                offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET,
                OmsClusterWireFormat.TYPE_ID_BATCH_ACCEPT_ORDER);
        buffer.putInt(
                offset + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET,
                OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(
                offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET,
                0L);
        buffer.putInt(offset + COUNT_OFFSET, count);
        return firstInnerOffset(offset);
    }

    /** Offset (relative to the start of the batch frame at {@code offset}) where inner commands begin. */
    public static int firstInnerOffset(int offset) {
        return offset + COUNT_OFFSET + COUNT_LENGTH;
    }

    /**
     * Append one inner {@link AcceptOrderCommand} body to a batch buffer at {@code dstOffset}.
     * The inner body must already be encoded into {@code src} starting at {@code srcOffset} for
     * {@code srcLength} bytes (typically by {@link AcceptOrderCommand#encode}).
     *
     * @return offset of the next inner-command-length prefix (i.e. {@code dstOffset + 4 + srcLength}).
     */
    public static int writeInner(
            MutableDirectBuffer dst,
            int dstOffset,
            DirectBuffer src,
            int srcOffset,
            int srcLength) {
        if (srcLength < OmsClusterWireFormat.HEADER_LENGTH || srcLength > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "inner AcceptOrderCommand length " + srcLength
                            + " out of bounds [" + OmsClusterWireFormat.HEADER_LENGTH
                            + ", " + OmsClusterWireFormat.MAX_COMMAND_BYTES + "]");
        }
        dst.putInt(dstOffset, srcLength);
        dst.putBytes(dstOffset + INNER_LENGTH_PREFIX_BYTES, src, srcOffset, srcLength);
        return dstOffset + INNER_LENGTH_PREFIX_BYTES + srcLength;
    }

    /**
     * Total wire length for a batch frame whose last inner-prefix-write returned
     * {@code lastInnerEndOffset} as its return value. Pass this to
     * {@link io.aeron.cluster.client.AeronCluster#offer(org.agrona.DirectBuffer, int, int)}'s
     * {@code length} argument.
     */
    public static int totalEncodedLength(int lastInnerEndOffset) {
        return lastInnerEndOffset;
    }

    /**
     * Read {@code count} from a batch frame. Caller has already validated {@code typeId} as
     * {@link OmsClusterWireFormat#TYPE_ID_BATCH_ACCEPT_ORDER} and {@code length} as
     * {@code &gt;= HEADER_LENGTH + COUNT_LENGTH}.
     */
    public static int readCount(DirectBuffer buffer, int offset) {
        int count = buffer.getInt(offset + COUNT_OFFSET);
        if (count < 1 || count > MAX_COUNT) {
            throw new IllegalArgumentException(
                    "batch count out of bounds [1, " + MAX_COUNT + "]: " + count);
        }
        return count;
    }

    /**
     * Visit each inner {@link AcceptOrderCommand} body in the batch by raw {@code (offset, length)}
     * tuples — no allocation on the hot path. The cluster service uses this to dispatch through
     * its existing {@link AcceptOrderCommand#decode} + {@code applyAcceptOrder} path once per
     * inner element, in batch order.
     *
     * @param buffer the batch frame buffer
     * @param offset start of the batch frame (the typeId byte)
     * @param length total bytes of the batch frame (typeId..last inner body inclusive)
     * @param visitor callback invoked once per inner element with {@code (innerOffset, innerLength)}
     * @throws IllegalArgumentException if the encoded layout is inconsistent (truncated frame,
     *         inner length out of bounds, count mismatch). Callers swallow these silently
     *         per {@link OmsAdmissionClusteredService#onSessionMessage} contract — malformed
     *         frames must not break determinism.
     */
    public static void forEachInner(
            DirectBuffer buffer, int offset, int length, InnerVisitor visitor) {
        int count = readCount(buffer, offset);
        int p = firstInnerOffset(offset);
        int end = offset + length;
        for (int i = 0; i < count; i++) {
            if (p + INNER_LENGTH_PREFIX_BYTES > end) {
                throw new IllegalArgumentException(
                        "batch truncated before inner-length-prefix " + i + " (got " + (end - p)
                                + " bytes, need at least " + INNER_LENGTH_PREFIX_BYTES + ")");
            }
            int innerLength = buffer.getInt(p);
            p += INNER_LENGTH_PREFIX_BYTES;
            if (innerLength < OmsClusterWireFormat.HEADER_LENGTH
                    || innerLength > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
                throw new IllegalArgumentException(
                        "batch inner[" + i + "] length " + innerLength + " out of bounds ["
                                + OmsClusterWireFormat.HEADER_LENGTH + ", "
                                + OmsClusterWireFormat.MAX_COMMAND_BYTES + "]");
            }
            if (p + innerLength > end) {
                throw new IllegalArgumentException(
                        "batch truncated mid-inner[" + i + "] (got " + (end - p)
                                + " bytes, need " + innerLength + ")");
            }
            visitor.accept(buffer, p, innerLength);
            p += innerLength;
        }
    }

    /** Callback for {@link #forEachInner}. */
    @FunctionalInterface
    public interface InnerVisitor {
        void accept(DirectBuffer buffer, int innerOffset, int innerLength);
    }
}
