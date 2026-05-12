package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip tests for {@link OrderRejectedEvent}.
 */
class OrderRejectedEventCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_shortReason_decodesIdenticalRecord() {
        OrderRejectedEvent original = new OrderRejectedEvent(
                /* correlationId = */ 99L,
                UUID.fromString("ffffffff-eeee-4ddd-8ccc-bbbbaaaa9999"),
                /* rejectCodeOrdinal = */ 4,
                /* rejectedAtNanos = */ 1_700_000_000_999_888_777L,
                "RISK_DUPLICATE");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        OrderRejectedEvent decoded = OrderRejectedEvent.decode(buf, DECODE_OFFSET);
        assertThat(decoded).isEqualTo(original);
        assertThat(written).isGreaterThan(OmsClusterWireFormat.HEADER_LENGTH);
    }

    @Test
    void roundTrip_emptyReason_decodesEmptyString() {
        OrderRejectedEvent original = new OrderRejectedEvent(
                1L, UUID.fromString("00000000-0000-4000-8000-000000000000"), 0, 0L, "");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        original.encode(buf, DECODE_OFFSET);

        OrderRejectedEvent decoded = OrderRejectedEvent.decode(buf, DECODE_OFFSET);
        assertThat(decoded.reason()).isEmpty();
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void encode_reasonExceedingMaxBytes_throws() {
        String tooLong = "y".repeat(OmsClusterWireFormat.MAX_STRING_BYTES + 1);
        OrderRejectedEvent ev = new OrderRejectedEvent(0L, UUID.randomUUID(), 0, 0L, tooLong);
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> ev.encode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds MAX_STRING_BYTES");
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);

        assertThatThrownBy(() -> OrderRejectedEvent.decode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }
}
