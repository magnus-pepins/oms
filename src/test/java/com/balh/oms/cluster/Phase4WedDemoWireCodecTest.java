package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Wire-format round-trip + invariants for the Wed-demo additions:
 * {@link RequestCancelOrderCommand}, {@link RequestReplaceOrderCommand},
 * {@link OrderCancelRequestedEvent}, {@link OrderReplaceRequestedEvent}, and the new
 * {@code EXEC_TYPE_REPLACE / EXEC_TYPE_CANCEL_REJECT / EXEC_TYPE_REPLACE_REJECT} discriminators
 * on {@link ApplyExecutionReportCommand}.
 *
 * <p>Each typeId pin catches accidental renumbering — bumping any of these reuses an ID for a
 * different type and corrupts the cluster log on every member that has already recorded events
 * with the prior meaning.
 */
class Phase4WedDemoWireCodecTest {

    private static final int BUF_CAPACITY = 1024;

    @Test
    void requestCancelOrder_roundTrip() {
        RequestCancelOrderCommand original = new RequestCancelOrderCommand(
                0xCAFEBABEL,
                UUID.fromString("11111111-2222-4333-9444-555555555555"),
                1_700_000_000_000_000_000L,
                "idem-req-abc-123",
                "user-cancel");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        RequestCancelOrderCommand decoded = RequestCancelOrderCommand.decode(buf, 0, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void requestCancelOrder_typeIdStable() {
        assertThat(OmsClusterWireFormat.TYPE_ID_REQUEST_CANCEL_ORDER).isEqualTo(5);
    }

    @Test
    void requestReplaceOrder_roundTrip() {
        RequestReplaceOrderCommand original = new RequestReplaceOrderCommand(
                42L,
                UUID.fromString("aaaaaaaa-bbbb-4ccc-9ddd-eeeeeeeeeeee"),
                /* newQuantityScaled = */ 1_500_000_000L,
                /* newLimitPriceScaledOrZero = */ 123_456_789L,
                /* requestedAtNanos = */ 1L,
                "idem-rep-xyz",
                "operator-resize");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        RequestReplaceOrderCommand decoded = RequestReplaceOrderCommand.decode(buf, 0, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void requestReplaceOrder_zeroPriceAllowed_meansKeepExistingLimitPrice() {
        RequestReplaceOrderCommand zeroPrice = new RequestReplaceOrderCommand(
                1L, UUID.randomUUID(), 100L, 0L, 0L, "k", "");
        assertThat(zeroPrice.newLimitPriceScaledOrZero()).isZero();
    }

    @Test
    void requestReplaceOrder_zeroQty_rejectedInConstructor() {
        assertThatThrownBy(
                () -> new RequestReplaceOrderCommand(1L, UUID.randomUUID(), 0L, 0L, 0L, "k", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requestReplaceOrder_typeIdStable() {
        assertThat(OmsClusterWireFormat.TYPE_ID_REQUEST_REPLACE_ORDER).isEqualTo(6);
    }

    @Test
    void orderCancelRequested_roundTrip() {
        OrderCancelRequestedEvent original = new OrderCancelRequestedEvent(
                UUID.fromString("11111111-2222-4333-9444-555555555555"),
                1_700_000_000_000L,
                /* shardId = */ 3,
                "acct-1",
                "AAPL",
                "idem-req-abc-123",
                "user-cancel");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        OrderCancelRequestedEvent decoded = OrderCancelRequestedEvent.decode(buf, 0, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void orderCancelRequested_typeIdStable() {
        assertThat(OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED).isEqualTo(1005);
    }

    @Test
    void orderReplaceRequested_roundTrip() {
        OrderReplaceRequestedEvent original = new OrderReplaceRequestedEvent(
                UUID.fromString("aaaaaaaa-bbbb-4ccc-9ddd-eeeeeeeeeeee"),
                /* originalQuantityScaled = */ 1_000_000_000L,
                /* originalLimitPriceScaledOrZero = */ 100_000_000L,
                /* newQuantityScaled = */ 1_500_000_000L,
                /* newLimitPriceScaledOrZero = */ 120_000_000L,
                /* requestedAtMillis = */ 1_700_000_000_000L,
                /* shardId = */ 1,
                /* sideCode = */ (byte) 1, // BUY (mirror of AcceptOrderCommand)
                /* timeInForceCode = */ (byte) 0,
                "acct-99",
                "TSLA",
                "idem-rep-xyz",
                "operator-resize");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        OrderReplaceRequestedEvent decoded = OrderReplaceRequestedEvent.decode(buf, 0, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void orderReplaceRequested_typeIdStable() {
        assertThat(OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED).isEqualTo(1006);
    }

    @Test
    void applyExecutionReport_execTypeReplace_roundTripsThroughExistingWire() {
        ApplyExecutionReportCommand original = new ApplyExecutionReportCommand(
                0L,
                UUID.fromString("11111111-2222-4333-9444-555555555555"),
                /* lastQtyScaled (overloaded: newTotalQty) = */ 1_500_000_000L,
                /* lastPxScaled (overloaded: newLimitPx) = */ 100_000_000L,
                1L,
                7,
                ApplyExecutionReportCommand.EXEC_TYPE_REPLACE,
                (byte) 0,
                "venue-1",
                "exec-ref-replace-1",
                "BROKER_ACCEPT",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"REPLACE\"}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        ApplyExecutionReportCommand decoded = ApplyExecutionReportCommand.decode(buf, 0, written);

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_REPLACE);
    }

    @Test
    void applyExecutionReport_execTypeCancelReject_roundTripsThroughExistingWire() {
        ApplyExecutionReportCommand original = new ApplyExecutionReportCommand(
                0L,
                UUID.fromString("11111111-2222-4333-9444-555555555555"),
                0L,
                0L,
                1L,
                9,
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT,
                (byte) 0,
                "venue-1",
                "ocr-broker-xyz-1",
                "BROKER_ACCEPT",
                "{\"kind\":\"OrderCancelReject\",\"cxlRejReason\":1}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(BUF_CAPACITY);
        int written = original.encode(buf, 0);
        ApplyExecutionReportCommand decoded = ApplyExecutionReportCommand.decode(buf, 0, written);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void execType_wireCodesStable() {
        // These bytes are written into the cluster log and snapshots. Renumbering them silently
        // corrupts replay (old bytes get a new meaning). Catches accidental shuffles.
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_TRADE).isEqualTo((byte) 0);
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL).isEqualTo((byte) 1);
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT).isEqualTo((byte) 2);
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_REPLACE).isEqualTo((byte) 3);
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT).isEqualTo((byte) 4);
        assertThat(ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT).isEqualTo((byte) 5);
    }
}
