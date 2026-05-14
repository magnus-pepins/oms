package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link BatchAcceptOrderCommand}.
 *
 * <p>Phase 4 Tier 2.5 phase D-6 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}:
 * the wire format is consumed by both the cluster client (encoder via the admit-batcher daemon)
 * and the cluster service (decoder in {@code OmsAdmissionClusteredService.applyBatchAcceptOrder}),
 * so corruption breaks log durability for every batched admit. The round-trip cases below cover
 * count-1, count-5, and count-MAX boundary cases plus malformed-frame guards.
 */
class BatchAcceptOrderCommandCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = OmsClusterWireFormat.MAX_BATCH_COMMAND_BYTES;
    private static final int FRAME_OFFSET = 0;

    private static AcceptOrderCommand sampleCommand(int seed) {
        return new AcceptOrderCommand(
                /* correlationId = */ 1_000_000L + seed,
                new UUID(0xCAFEBABEL, seed),
                /* clientTimestampNanos = */ 1_700_000_000_000_000_000L + seed,
                /* quantityScaled = */ 1_000_000_000L * (seed + 1),
                /* limitPriceScaledOrZero = */ 100_000L + seed,
                /* shardId = */ seed % 4,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-" + seed,
                "idem-" + seed,
                "hash-" + seed,
                "AAPL",
                seed % 2 == 0 ? "ledger-bal-" + seed : null);
    }

    @Test
    void roundTrip_singleElementBatch_decodesIdenticalInner() {
        AcceptOrderCommand inner = sampleCommand(7);

        ExpandableArrayBuffer perCmd = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int innerLen = inner.encode(perCmd, 0);

        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        p = BatchAcceptOrderCommand.writeInner(batch, p, perCmd, 0, innerLen);
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, /* count = */ 1);
        int totalBytes = BatchAcceptOrderCommand.totalEncodedLength(p);

        // Outer-frame asserts
        assertThat(batch.getInt(FRAME_OFFSET + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET))
                .isEqualTo(OmsClusterWireFormat.TYPE_ID_BATCH_ACCEPT_ORDER);
        assertThat(batch.getInt(FRAME_OFFSET + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET))
                .isEqualTo(OmsClusterWireFormat.SCHEMA_VERSION);
        assertThat(BatchAcceptOrderCommand.readCount(batch, FRAME_OFFSET)).isEqualTo(1);

        // Decoded inner round-trip
        List<AcceptOrderCommand> decoded = new ArrayList<>();
        BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes,
                (buf, off, len) -> decoded.add(AcceptOrderCommand.decode(buf, off, len)));
        assertThat(decoded).containsExactly(inner);
    }

    @Test
    void roundTrip_fiveElementBatch_preservesOrderAndAllFields() {
        List<AcceptOrderCommand> originals = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            originals.add(sampleCommand(100 + i));
        }

        ExpandableArrayBuffer perCmd = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        for (AcceptOrderCommand c : originals) {
            int innerLen = c.encode(perCmd, 0);
            p = BatchAcceptOrderCommand.writeInner(batch, p, perCmd, 0, innerLen);
        }
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, originals.size());
        int totalBytes = BatchAcceptOrderCommand.totalEncodedLength(p);

        List<AcceptOrderCommand> decoded = new ArrayList<>();
        BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes,
                (buf, off, len) -> decoded.add(AcceptOrderCommand.decode(buf, off, len)));

        assertThat(decoded).containsExactlyElementsOf(originals);
    }

    @Test
    void roundTrip_maxCountBatch_decodesAllInners() {
        int count = BatchAcceptOrderCommand.MAX_COUNT;
        // Use small fixed-size strings so 256 inner commands stay under MAX_BATCH_COMMAND_BYTES.
        ExpandableArrayBuffer perCmd = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        for (int i = 0; i < count; i++) {
            AcceptOrderCommand c = new AcceptOrderCommand(
                    i, new UUID(0L, i), 0L, 1_000L, 0L, 0,
                    AcceptOrderCommand.SIDE_SELL, AcceptOrderCommand.TIF_IOC,
                    "a", "i", "h", "X", null);
            int innerLen = c.encode(perCmd, 0);
            p = BatchAcceptOrderCommand.writeInner(batch, p, perCmd, 0, innerLen);
        }
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, count);
        int totalBytes = BatchAcceptOrderCommand.totalEncodedLength(p);

        AtomicInteger decodedCount = new AtomicInteger();
        BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes, (buf, off, len) -> {
            AcceptOrderCommand inner = AcceptOrderCommand.decode(buf, off, len);
            assertThat(inner.correlationId()).isEqualTo((long) decodedCount.getAndIncrement());
        });
        assertThat(decodedCount.get()).isEqualTo(count);
    }

    @Test
    void writeHeader_countOutOfRange_throws() {
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        assertThatThrownBy(() -> BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count out of bounds");
        assertThatThrownBy(
                () -> BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET,
                        BatchAcceptOrderCommand.MAX_COUNT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count out of bounds");
    }

    @Test
    void writeInner_innerLengthBelowHeader_throws() {
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        ExpandableArrayBuffer src = new ExpandableArrayBuffer(64);
        assertThatThrownBy(() -> BatchAcceptOrderCommand.writeInner(batch, 0, src, 0,
                OmsClusterWireFormat.HEADER_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inner AcceptOrderCommand length");
    }

    @Test
    void forEachInner_truncatedFrameMidLengthPrefix_throws() {
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, 1);
        // No inner-length prefix written: frame length is just the header + count.
        int totalBytes = OmsClusterWireFormat.HEADER_LENGTH + BatchAcceptOrderCommand.COUNT_LENGTH;

        assertThatThrownBy(
                () -> BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes,
                        (buf, off, len) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("truncated before inner-length-prefix");
    }

    @Test
    void forEachInner_truncatedFrameMidInnerBody_throws() {
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, 1);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        // Claim a 200-byte inner body but stop the frame at the prefix only.
        batch.putInt(p, 200);
        int totalBytes = p + BatchAcceptOrderCommand.INNER_LENGTH_PREFIX_BYTES + 50; // truncate

        assertThatThrownBy(
                () -> BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes,
                        (buf, off, len) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("truncated mid-inner");
    }

    @Test
    void forEachInner_innerLengthOutOfBounds_throws() {
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, 1);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        batch.putInt(p, OmsClusterWireFormat.MAX_COMMAND_BYTES + 1);
        int totalBytes = p + BatchAcceptOrderCommand.INNER_LENGTH_PREFIX_BYTES
                + OmsClusterWireFormat.MAX_COMMAND_BYTES + 1;

        assertThatThrownBy(
                () -> BatchAcceptOrderCommand.forEachInner(batch, FRAME_OFFSET, totalBytes,
                        (buf, off, len) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of bounds");
    }
}
