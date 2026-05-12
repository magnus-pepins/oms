package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link AcceptOrderCommand}.
 *
 * <p>The wire format is consumed by every cluster member (leader, followers, and
 * any future replay tooling) so corruption here breaks log durability across the
 * whole cluster. This suite is intentionally exhaustive on field coverage.
 */
class AcceptOrderCommandCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_allFieldsPresent_decodesIdenticalRecord() {
        AcceptOrderCommand original = new AcceptOrderCommand(
                /* correlationId = */ 0xDEAD_BEEF_1234L,
                UUID.fromString("a1b2c3d4-e5f6-4789-9abc-def012345678"),
                /* clientTimestampNanos = */ 1_700_000_000_000_000_000L,
                /* quantityScaled = */ 10_500_000_000L,
                /* limitPriceScaledOrZero = */ 123_456_000L,
                /* shardId = */ 7,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_GTC,
                "acct-12345",
                "client-idem-9876",
                "hash:" + "a".repeat(60),
                "TSLA",
                "ledger-bal-42");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        AcceptOrderCommand decoded = AcceptOrderCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_marketOrderWithNoLedgerBalanceId_decodesNullField() {
        AcceptOrderCommand original = new AcceptOrderCommand(
                42L,
                UUID.fromString("00000000-0000-4000-8000-000000000001"),
                1L,
                1_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                0,
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_IOC,
                "acct",
                "idem",
                "hash",
                "BTC",
                /* ledgerBalanceIdOrNull = */ null);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        AcceptOrderCommand decoded = AcceptOrderCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded.ledgerBalanceIdOrNull()).isNull();
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void encode_stringExceedingMaxBytes_throwsBeforeAnyWireWrite() {
        String tooLong = "x".repeat(OmsClusterWireFormat.MAX_STRING_BYTES + 1);
        AcceptOrderCommand cmd = new AcceptOrderCommand(
                1L, UUID.randomUUID(), 1L, 1L, 0L, 0,
                AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY,
                tooLong, "idem", "hash", "AAPL", null);
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> cmd.encode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds MAX_STRING_BYTES");
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> AcceptOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void decode_unsupportedSchemaVersion_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION + 1);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> AcceptOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported schema version");
    }

    @Test
    void decode_bufferShorterThanHeader_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(
                () -> AcceptOrderCommand.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for header");
    }
}
