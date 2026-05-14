package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link OrderCancelAppliedEvent} (slice 4p).
 */
class OrderCancelAppliedEventCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_allFieldsPresent_decodesIdenticalRecord() {
        OrderCancelAppliedEvent original = new OrderCancelAppliedEvent(
                UUID.fromString("a1b2c3d4-e5f6-4789-9abc-def012345678"),
                /* cancelledAtMillis = */ 1_700_000_000_000L,
                /* newVersion = */ 7,
                /* shardId = */ 0,
                "acct-12345",
                "hash:" + "f".repeat(60),
                "TSLA",
                "ledger_inflight_hold_failed:insufficient_funds");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        OrderCancelAppliedEvent decoded = OrderCancelAppliedEvent.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_emptyReason_decodesIdentical() {
        OrderCancelAppliedEvent original = new OrderCancelAppliedEvent(
                UUID.fromString("00000000-0000-4000-8000-000000000001"),
                42L,
                1,
                0,
                "acct",
                "hash",
                "AAPL",
                "");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        OrderCancelAppliedEvent decoded = OrderCancelAppliedEvent.decode(buf, DECODE_OFFSET, written);
        assertThat(decoded.reason()).isEmpty();
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION);
        buf.putLong(8, 0L);

        assertThatThrownBy(
                () -> OrderCancelAppliedEvent.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void typeId_isStable() {
        // Slice 4p commits to TYPE_ID_ORDER_CANCEL_APPLIED=1004. Reuse breaks projector replay.
        assertThat(OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_APPLIED).isEqualTo(1004);
    }

    @Test
    void encode_writesZeroCorrelationIdInHeader() {
        // Side-publication events do not carry a correlation id (projectors consume by log
        // position). The cluster service relies on this so a downstream consumer reading the
        // header in isolation does not interpret a stale stack value as a correlation id.
        OrderCancelAppliedEvent ev = new OrderCancelAppliedEvent(
                UUID.randomUUID(), 1L, 1, 0, "a", "h", "X", "");
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        ev.encode(buf, DECODE_OFFSET);

        long correlationOnWire = buf.getLong(DECODE_OFFSET + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);
        assertThat(correlationOnWire).isZero();
    }
}
