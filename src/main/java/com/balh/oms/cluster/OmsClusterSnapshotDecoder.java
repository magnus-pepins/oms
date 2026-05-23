package com.balh.oms.cluster;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Read-only snapshot decoder usable from operator tools that need to introspect the cluster's
 * persisted state without joining the consensus ring.
 *
 * <p>Closes gap 1 of the post-V55/V56 stability review (see
 * {@code system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md}
 * §10). Mirrors the wire shape that {@code OmsAdmissionClusteredService.SnapshotLoader.onFragment}
 * understands — same magic, same schema version check, same {@link
 * OmsAdmissionClusteredService.AdmittedOrder#decode} call, same string framing for execution
 * refs / sender-seq blocks. The cluster's loader populates in-memory maps; this decoder fires
 * per-record callbacks so callers (e.g. the projector-rebuild-from-snapshot operator tool) can
 * stream-process the contents into Postgres / JSON / metrics without holding the whole snapshot
 * in memory.
 *
 * <p>The decoder DELIBERATELY does not share code with {@code SnapshotLoader.onFragment}: the
 * 2026-05-23 pop incident was a snapshot-format-handling bug, and any refactor that reaches
 * inside the cluster's load path is high-risk. The duplication here is small enough (one
 * fragment handler) that drift between the two decoders will be caught by the round-trip test
 * {@code OmsClusterSnapshotDecoderTest.encodeThenDecode_roundTripsAdmittedOrders}, which encodes
 * via the production {@code AdmittedOrder.encode} + cluster's own snapshot framing and decodes
 * via this class.
 *
 * <p>Each callback receives a {@code AdmittedOrder} as the cluster currently sees it (with its
 * live {@code statusCode} and {@code cumQtyScaled} — NOT the original admit-time NEW state).
 * Tools that re-hydrate Postgres MUST honour those fields if they want the projector view to
 * match cluster reality; using the orders-INSERT path that always sets {@code status=NEW} would
 * regress filled / cancelled orders back to NEW.
 */
public final class OmsClusterSnapshotDecoder implements FragmentHandler {

    private final Consumer<OmsAdmissionClusteredService.AdmittedOrder> onOrder;

    /** Optional execution-ref callback. Most tools ignore this; set to {@code null} to skip. */
    private final ExecutionRefConsumer onExecutionRefs;

    /** Lifetime totals exposed for the operator tool's summary line. */
    private long ordersDecoded;
    private long executionRefBlocksDecoded;

    public OmsClusterSnapshotDecoder(
            Consumer<OmsAdmissionClusteredService.AdmittedOrder> onOrder,
            ExecutionRefConsumer onExecutionRefs) {
        this.onOrder = onOrder;
        this.onExecutionRefs = onExecutionRefs;
    }

    public long ordersDecoded() {
        return ordersDecoded;
    }

    public long executionRefBlocksDecoded() {
        return executionRefBlocksDecoded;
    }

    /**
     * Decode one snapshot fragment. The caller MUST wrap this in
     * {@link io.aeron.ImageFragmentAssembler} so multi-fragment snapshots are reassembled
     * before the magic+length header is validated — same constraint as
     * {@code SnapshotLoader.onFragment}; the 2026-05-23 pop incident proved that the
     * pre-assembler shape crashes once cluster state exceeds the Aeron MTU.
     */
    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        int p = offset;
        int magic = buffer.getInt(p);
        p += Integer.BYTES;
        if (magic != OmsAdmissionClusteredService.SNAPSHOT_MAGIC) {
            throw new IllegalStateException("snapshot magic mismatch: 0x" + Integer.toHexString(magic));
        }
        int schemaVersion = buffer.getInt(p);
        p += Integer.BYTES;
        if (schemaVersion != OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION) {
            throw new IllegalStateException(
                    "unsupported snapshot schema version " + schemaVersion
                            + " (this build understands " + OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION
                            + "); rebuild the tool from a matching cluster revision or wait for a snapshot from this build");
        }
        int count = buffer.getInt(p);
        p += Integer.BYTES;
        for (int i = 0; i < count; i++) {
            OmsAdmissionClusteredService.AdmittedOrder o =
                    OmsAdmissionClusteredService.AdmittedOrder.decode(buffer, p);
            p += o.encodedLength();
            ordersDecoded++;
            onOrder.accept(o);
        }
        int orderCountWithRefs = buffer.getInt(p);
        p += Integer.BYTES;
        for (int i = 0; i < orderCountWithRefs; i++) {
            long msb = buffer.getLong(p);
            p += Long.BYTES;
            long lsb = buffer.getLong(p);
            p += Long.BYTES;
            int refCount = buffer.getInt(p);
            p += Integer.BYTES;
            Set<String> refs = new HashSet<>(refCount);
            for (int j = 0; j < refCount; j++) {
                int refLen = buffer.getInt(p);
                byte[] bytes = new byte[refLen];
                buffer.getBytes(p + Integer.BYTES, bytes);
                refs.add(new String(bytes, StandardCharsets.UTF_8));
                p += Integer.BYTES + refLen;
            }
            executionRefBlocksDecoded++;
            if (onExecutionRefs != null) {
                onExecutionRefs.accept(new UUID(msb, lsb), refs);
            }
        }
        // senderSeqIndex (v3) — read past to validate framing, but the rebuild tool doesn't
        // need it (FIX-session dedupe is not a Postgres-side concern).
        int senderCountWithSeqs = buffer.getInt(p);
        p += Integer.BYTES;
        for (int i = 0; i < senderCountWithSeqs; i++) {
            int senderLen = buffer.getInt(p);
            p += Integer.BYTES + senderLen;
            int seqCount = buffer.getInt(p);
            p += Integer.BYTES;
            p += Integer.BYTES * seqCount;
        }
    }

    @FunctionalInterface
    public interface ExecutionRefConsumer {
        void accept(UUID orderId, Set<String> executionRefs);
    }
}
