package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip tests for {@link OrderAcceptedEvent}, the cluster egress event
 * subscribers (cluster client, Postgres projector, FIX egress) decode.
 */
class OrderAcceptedEventCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 256;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_freshAccept_preservesAllFields() {
        OrderAcceptedEvent original = new OrderAcceptedEvent(
                /* correlationId = */ 0xCAFE_BABE_1L,
                UUID.fromString("00000000-0000-4000-8000-000000000123"),
                /* version = */ 0,
                /* duplicate = */ false,
                /* acceptedAtNanos = */ 1_700_000_000_111_222_333L);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        assertThat(written).isEqualTo(OrderAcceptedEvent.ENCODED_LENGTH);
        OrderAcceptedEvent decoded = OrderAcceptedEvent.decode(buf, DECODE_OFFSET);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_idempotentReHit_duplicateBitTrue() {
        OrderAcceptedEvent original = new OrderAcceptedEvent(
                7L,
                UUID.fromString("11111111-2222-4333-8444-555555555555"),
                3,
                /* duplicate = */ true,
                42L);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        original.encode(buf, DECODE_OFFSET);

        OrderAcceptedEvent decoded = OrderAcceptedEvent.decode(buf, DECODE_OFFSET);
        assertThat(decoded.duplicate()).isTrue();
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);

        assertThatThrownBy(() -> OrderAcceptedEvent.decode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void encodedLength_matchesConstant() {
        OrderAcceptedEvent ev = new OrderAcceptedEvent(0L, UUID.randomUUID(), 0, false, 0L);
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThat(ev.encode(buf, DECODE_OFFSET)).isEqualTo(OrderAcceptedEvent.ENCODED_LENGTH);
    }
}
