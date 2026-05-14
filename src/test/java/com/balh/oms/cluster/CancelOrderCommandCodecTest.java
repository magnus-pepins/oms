package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link CancelOrderCommand} (slice 4p).
 */
class CancelOrderCommandCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_allFieldsPresent_decodesIdenticalRecord() {
        CancelOrderCommand original = new CancelOrderCommand(
                /* correlationId = */ 0xCAFEBABE_F00DL,
                UUID.fromString("11111111-2222-4333-9444-555555555555"),
                /* requestedAtNanos = */ 1_700_000_000_000_000_000L,
                "ledger_inflight_hold_failed:insufficient_funds:acct-42");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        CancelOrderCommand decoded = CancelOrderCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_emptyReason_decodesIdentical() {
        CancelOrderCommand original = new CancelOrderCommand(
                7L,
                UUID.fromString("00000000-0000-4000-8000-000000000001"),
                42L,
                "");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        CancelOrderCommand decoded = CancelOrderCommand.decode(buf, DECODE_OFFSET, written);
        assertThat(decoded.reason()).isEmpty();
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void encode_reasonExceedingMaxBytes_throwsBeforeWireWrite() {
        String tooLong = "x".repeat(OmsClusterWireFormat.MAX_STRING_BYTES + 1);
        CancelOrderCommand cmd = new CancelOrderCommand(1L, UUID.randomUUID(), 1L, tooLong);
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> cmd.encode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds MAX_STRING_BYTES");
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> CancelOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void decode_unsupportedSchemaVersion_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_CANCEL_ORDER);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION + 1);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> CancelOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported schema version");
    }

    @Test
    void decode_bufferShorterThanHeader_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(
                () -> CancelOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for header");
    }

    @Test
    void typeId_isStable() {
        // Slice 4p commits to TYPE_ID_CANCEL_ORDER=3 on the wire. Bumping this number reuses an
        // ID for a different type — which corrupts the cluster log on every member that has
        // already recorded events with the prior meaning. Catches accidental renumbering.
        assertThat(OmsClusterWireFormat.TYPE_ID_CANCEL_ORDER).isEqualTo(3);
    }
}
