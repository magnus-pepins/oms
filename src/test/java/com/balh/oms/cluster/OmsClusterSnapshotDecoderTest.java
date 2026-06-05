package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closes gap 1 test coverage: encodes a known {@code orderIndex} into the cluster's snapshot
 * wire format (mirrors {@code OmsAdmissionClusteredService.onTakeSnapshot}) and asserts the
 * standalone {@link OmsClusterSnapshotDecoder} decodes the same orders back. This protects
 * against drift between the cluster's loader and the operator-tool decoder, which would
 * otherwise be silent until an actual rebuild ran on a real snapshot file.
 */
class OmsClusterSnapshotDecoderTest {

    @Test
    void encodeThenDecode_roundTripsAdmittedOrders() {
        OmsAdmissionClusteredService.AdmittedOrder o1 = order(
                /* statusCode = */ (byte) 2,  // WORKING
                /* limitPriceScaled = */ 15_000_000_000L,  // $150.00 at 1e8 scale
                /* cumQty = */ 0L,
                /* shardId = */ 0,
                /* tif = */ (byte) 1,  // DAY
                /* side = */ (byte) 1, // BUY
                /* ledgerBalanceId = */ "bal-1");
        OmsAdmissionClusteredService.AdmittedOrder o2 = order(
                /* statusCode = */ (byte) 3,  // PARTIALLY_FILLED
                /* limitPriceScaled = */ 0L,
                /* cumQty = */ 50_000_000L,   // 0.5 of a share
                /* shardId = */ 1,
                /* tif = */ (byte) 2,  // GTC
                /* side = */ (byte) 2, // SELL
                /* ledgerBalanceId = */ null);

        byte[] snapshotBytes = encodeSnapshot(List.of(o1, o2));

        List<OmsAdmissionClusteredService.AdmittedOrder> decoded = new ArrayList<>();
        OmsClusterSnapshotDecoder decoder = new OmsClusterSnapshotDecoder(decoded::add, /* refs = */ null);

        decoder.onFragment(new UnsafeBuffer(snapshotBytes), 0, snapshotBytes.length, null);

        assertThat(decoder.ordersDecoded()).isEqualTo(2);
        assertThat(decoded).hasSize(2);
        assertAdmittedOrderEquals(o1, decoded.get(0));
        assertAdmittedOrderEquals(o2, decoded.get(1));
    }

    @Test
    void executionRefBlocks_invokeExecutionRefConsumerAndAdvanceCursorCorrectly() {
        OmsAdmissionClusteredService.AdmittedOrder o = order((byte) 2, 100L, 0L, 0, (byte) 1, (byte) 1, "bal");
        UUID orderId = o.orderId();
        Set<String> refsForO = new HashSet<>(Set.of("E-1", "E-2"));

        byte[] snapshotBytes = encodeSnapshotWithRefs(
                List.of(o),
                List.of(new RefBlock(orderId, refsForO)));

        List<UUID> seenOrderIds = new ArrayList<>();
        List<Set<String>> seenRefs = new ArrayList<>();
        OmsClusterSnapshotDecoder decoder = new OmsClusterSnapshotDecoder(
                ignored -> {},
                (uuid, refs) -> { seenOrderIds.add(uuid); seenRefs.add(refs); });

        decoder.onFragment(new UnsafeBuffer(snapshotBytes), 0, snapshotBytes.length, null);

        assertThat(decoder.executionRefBlocksDecoded()).isEqualTo(1);
        assertThat(seenOrderIds).containsExactly(orderId);
        assertThat(seenRefs).hasSize(1);
        assertThat(seenRefs.get(0)).containsExactlyInAnyOrder("E-1", "E-2");
    }

    @Test
    void wrongMagic_failsLoud() {
        byte[] bytes = new byte[12];
        UnsafeBuffer b = new UnsafeBuffer(bytes);
        b.putInt(0, 0xDEADBEEF);
        b.putInt(4, OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION);
        b.putInt(8, 0);

        OmsClusterSnapshotDecoder decoder = new OmsClusterSnapshotDecoder(ignored -> {}, null);

        assertThatThrownBy(() -> decoder.onFragment(b, 0, bytes.length, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot magic mismatch");
    }

    @Test
    void wrongSchemaVersion_failsLoudWithCurrentVersion() {
        byte[] bytes = new byte[12];
        UnsafeBuffer b = new UnsafeBuffer(bytes);
        b.putInt(0, OmsAdmissionClusteredService.SNAPSHOT_MAGIC);
        b.putInt(4, 999);
        b.putInt(8, 0);

        OmsClusterSnapshotDecoder decoder = new OmsClusterSnapshotDecoder(ignored -> {}, null);

        assertThatThrownBy(() -> decoder.onFragment(b, 0, bytes.length, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unsupported snapshot schema version 999")
                .hasMessageContaining(String.valueOf(OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION));
    }

    // --------------------------------------------------------------------------------------
    // Helpers — mirror onTakeSnapshot's framing exactly. Kept here (not in production) so a
    // drift between the cluster's onTakeSnapshot and the decoder is caught by this test
    // instead of corrupting a real snapshot rebuild on pop.
    // --------------------------------------------------------------------------------------

    private static OmsAdmissionClusteredService.AdmittedOrder order(
            byte statusCode,
            long limitPriceScaled,
            long cumQty,
            int shardId,
            byte tif,
            byte side,
            String ledgerBalanceId) {
        return new OmsAdmissionClusteredService.AdmittedOrder(
                UUID.randomUUID(),
                UUID.randomUUID().toString(),
                "client-idem-" + System.nanoTime(),
                "hash-" + System.nanoTime(),
                "AAPL",
                side,
                /* quantityScaled = */ 100_000_000L, // 1.0 share
                limitPriceScaled,
                tif,
                ledgerBalanceId,
                /* version = */ 0,
                /* acceptedAtMillis = */ 1_700_000_000_000L,
                statusCode,
                cumQty,
                shardId,
                -1L);
    }

    private static byte[] encodeSnapshot(List<OmsAdmissionClusteredService.AdmittedOrder> orders) {
        return encodeSnapshotWithRefs(orders, List.of());
    }

    private static byte[] encodeSnapshotWithRefs(
            List<OmsAdmissionClusteredService.AdmittedOrder> orders,
            List<RefBlock> refBlocks) {
        MutableDirectBuffer buf = new ExpandableArrayBuffer(4096);
        int p = 0;
        buf.putInt(p, OmsAdmissionClusteredService.SNAPSHOT_MAGIC);
        p += Integer.BYTES;
        buf.putInt(p, OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION);
        p += Integer.BYTES;
        buf.putInt(p, orders.size());
        p += Integer.BYTES;
        for (OmsAdmissionClusteredService.AdmittedOrder o : orders) {
            int endP = o.encode(buf, p);
            p = endP;
        }
        // execution refs
        buf.putInt(p, refBlocks.size());
        p += Integer.BYTES;
        for (RefBlock rb : refBlocks) {
            buf.putLong(p, rb.orderId.getMostSignificantBits());
            p += Long.BYTES;
            buf.putLong(p, rb.orderId.getLeastSignificantBits());
            p += Long.BYTES;
            buf.putInt(p, rb.refs.size());
            p += Integer.BYTES;
            for (String ref : rb.refs) {
                byte[] bytes = ref.getBytes(StandardCharsets.UTF_8);
                buf.putInt(p, bytes.length);
                buf.putBytes(p + Integer.BYTES, bytes);
                p += Integer.BYTES + bytes.length;
            }
        }
        // empty senderSeqIndex (v3 framing — count = 0).
        buf.putInt(p, 0);
        p += Integer.BYTES;

        byte[] out = new byte[p];
        buf.getBytes(0, out);
        return out;
    }

    private static void assertAdmittedOrderEquals(
            OmsAdmissionClusteredService.AdmittedOrder expected,
            OmsAdmissionClusteredService.AdmittedOrder actual) {
        assertThat(actual.orderId()).isEqualTo(expected.orderId());
        assertThat(actual.accountId()).isEqualTo(expected.accountId());
        assertThat(actual.clientIdempotencyKey()).isEqualTo(expected.clientIdempotencyKey());
        assertThat(actual.accountIdHash()).isEqualTo(expected.accountIdHash());
        assertThat(actual.instrumentSymbol()).isEqualTo(expected.instrumentSymbol());
        assertThat(actual.side()).isEqualTo(expected.side());
        assertThat(actual.quantityScaled()).isEqualTo(expected.quantityScaled());
        assertThat(actual.limitPriceScaledOrZero()).isEqualTo(expected.limitPriceScaledOrZero());
        assertThat(actual.timeInForceCode()).isEqualTo(expected.timeInForceCode());
        assertThat(actual.ledgerBalanceIdOrNull()).isEqualTo(expected.ledgerBalanceIdOrNull());
        assertThat(actual.version()).isEqualTo(expected.version());
        assertThat(actual.acceptedAtMillis()).isEqualTo(expected.acceptedAtMillis());
        assertThat(actual.statusCode()).isEqualTo(expected.statusCode());
        assertThat(actual.cumQtyScaled()).isEqualTo(expected.cumQtyScaled());
        assertThat(actual.shardId()).isEqualTo(expected.shardId());
    }

    private record RefBlock(UUID orderId, Set<String> refs) {}
}
