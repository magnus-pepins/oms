package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip tests for {@link OrderAdmittedEvent} — the projection event consumed by
 * {@link com.balh.oms.projector.OmsPostgresProjector} (Phase 2 of the Aeron Cluster substrate plan).
 */
class OrderAdmittedEventCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_freshAccept_withLedgerBalance_preservesAllFields() {
        OrderAdmittedEvent original = new OrderAdmittedEvent(
                UUID.fromString("00000000-0000-4000-8000-000000000123"),
                /* clientTimestampNanos = */ 1_700_000_000_111_222_333L,
                /* acceptedAtMillis = */ 1_700_000_000_999L,
                /* quantityScaled = */ 10_500_000_000L,
                /* limitPriceScaledOrZero = */ 250_500_000L,
                /* shardId = */ 7,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-1",
                "idem-1",
                "hash-1",
                "AAPL",
                "ledger-1");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        OrderAdmittedEvent decoded = OrderAdmittedEvent.decode(buf, DECODE_OFFSET, written);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_marketOrder_noLedgerBalance() {
        OrderAdmittedEvent original = new OrderAdmittedEvent(
                UUID.fromString("11111111-2222-4333-8444-555555555555"),
                100L,
                200L,
                /* quantityScaled = */ 5L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_IOC,
                "acct-2",
                "idem-2",
                "hash-2",
                "MSFT",
                /* ledgerBalanceIdOrNull = */ null);

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        OrderAdmittedEvent decoded = OrderAdmittedEvent.decode(buf, DECODE_OFFSET, written);
        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.ledgerBalanceIdOrNull()).isNull();
    }

    @Test
    void fromAdmittedCommand_carriesAllCommandFieldsPlusClusterTimestampAndVersion() {
        AcceptOrderCommand cmd = new AcceptOrderCommand(
                /* correlationId = */ 999L,
                UUID.fromString("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"),
                /* clientTimestampNanos = */ 1_000_000L,
                /* quantityScaled = */ 7_000_000_000L,
                /* limitPriceScaledOrZero = */ 100_000L,
                /* shardId = */ 3,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_GTC,
                "acct-3",
                "idem-3",
                "hash-3",
                "GOOG",
                "ledger-3");

        OrderAdmittedEvent ev = OrderAdmittedEvent.fromAdmittedCommand(cmd, /* acceptedAtMillis = */ 5_000_000L, 0);

        assertThat(ev.orderId()).isEqualTo(cmd.orderId());
        assertThat(ev.clientTimestampNanos()).isEqualTo(cmd.clientTimestampNanos());
        assertThat(ev.acceptedAtMillis()).isEqualTo(5_000_000L);
        assertThat(ev.quantityScaled()).isEqualTo(cmd.quantityScaled());
        assertThat(ev.limitPriceScaledOrZero()).isEqualTo(cmd.limitPriceScaledOrZero());
        assertThat(ev.shardId()).isEqualTo(cmd.shardId());
        assertThat(ev.version()).isZero();
        assertThat(ev.side()).isEqualTo(cmd.side());
        assertThat(ev.timeInForceCode()).isEqualTo(cmd.timeInForceCode());
        assertThat(ev.accountId()).isEqualTo(cmd.accountId());
        assertThat(ev.clientIdempotencyKey()).isEqualTo(cmd.clientIdempotencyKey());
        assertThat(ev.accountIdHash()).isEqualTo(cmd.accountIdHash());
        assertThat(ev.instrumentSymbol()).isEqualTo(cmd.instrumentSymbol());
        assertThat(ev.ledgerBalanceIdOrNull()).isEqualTo(cmd.ledgerBalanceIdOrNull());
    }

    @Test
    void decode_wrongTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);
        buf.putInt(OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);

        assertThatThrownBy(() -> OrderAdmittedEvent.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void decode_wrongSchemaVersion_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED);
        buf.putInt(OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION + 99);

        assertThatThrownBy(() -> OrderAdmittedEvent.decode(buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema version");
    }
}
