package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import quickfix.field.ClOrdID;
import quickfix.field.HandlInst;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.fix44.OrderCancelReplaceRequest;
import quickfix.fix44.OrderCancelRequest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Field-shape coverage for the Wed-demo cancel/replace FIX builders. Pins:
 *
 * <ul>
 *   <li>{@code OrigClOrdID(41)} matches the order's UUID (= the original NOS's ClOrdID).</li>
 *   <li>{@code ClOrdID(11)} is deterministic from {@code (orderId, clientRequestKey)} — guards
 *       the replay-redelivery dedupe contract documented on
 *       {@link FixOrderCancelRequestBuilder#deriveCancelClOrdID}.</li>
 *   <li>{@code Side(54)} / {@code Symbol(55)} / {@code OrderQty(38)} / {@code OrdType(40)} /
 *       {@code Price(44)} / {@code HandlInst(21)} / {@code TimeInForce(59)} round-trip
 *       correctly, including the "qty-only modify ⇒ copy original price" branch on 35=G.</li>
 * </ul>
 */
class FixCancelReplaceBuildersTest {

    private static final UUID ORDER_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

    private FixSymbolMapper symbolMapper() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setSymbolMapJson("{\"AAPL\":\"NASDAQ.AAPL\"}");
        return new FixSymbolMapper(cfg, new ObjectMapper());
    }

    // ---- 35=F OrderCancelRequest --------------------------------------------------------

    @Test
    void cancelBuilder_buildsFromEvent_withDeterministicClOrdID() throws Exception {
        FixOrderCancelRequestBuilder builder = new FixOrderCancelRequestBuilder(symbolMapper());
        OrderCancelRequestedEvent event = new OrderCancelRequestedEvent(
                ORDER_ID,
                /* originalQuantityScaled = */ 2_500_000_000L,
                /* cumQtyScaled = */ 500_000_000L,
                /* requestedAtMillis = */ 1_700_000_000_000L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_SELL,
                "acct-1",
                "AAPL",
                "idem-cancel-7",
                "operator-cancel");

        OrderCancelRequest msg = builder.build(event);

        assertThat(msg.get(new OrigClOrdID()).getValue()).isEqualTo(ORDER_ID.toString());
        assertThat(msg.get(new ClOrdID()).getValue())
                .as("ClOrdID derived deterministically — replay-redelivered events must collide via DupClOrdID")
                .isEqualTo(ORDER_ID + ":c:idem-cancel-7");
        assertThat(msg.get(new Symbol()).getValue())
                .as("symbol mapper is honored on the cancel side too")
                .isEqualTo("NASDAQ.AAPL");
        assertThat(msg.get(new Side()).getValue()).isEqualTo(Side.SELL);
        assertThat(msg.getString(OrderQty.FIELD))
                .as("FIX 4.4 OrderCancelRequest requires OrderQty(38); we send the original NOS qty")
                .isEqualTo("2.5");
    }

    @Test
    void cancelBuilder_emptyClientRequestKey_stillProducesUniqueClOrdID() throws Exception {
        FixOrderCancelRequestBuilder builder = new FixOrderCancelRequestBuilder(symbolMapper());
        OrderCancelRequestedEvent event = new OrderCancelRequestedEvent(
                ORDER_ID,
                1_000_000_000L,
                0L,
                0L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                "acct-1",
                "AAPL",
                "",
                "");

        OrderCancelRequest msg = builder.build(event);
        assertThat(msg.get(new ClOrdID()).getValue())
                .as("empty clientRequestKey ⇒ orderId + ':c:' suffix — distinct from any keyed cancel ClOrdID")
                .isEqualTo(ORDER_ID + ":c:");
    }

    // ---- 35=G OrderCancelReplaceRequest -------------------------------------------------

    @Test
    void replaceBuilder_qtyAndPrice_buildsLimitOrder_withNewValues() throws Exception {
        FixOrderCancelReplaceRequestBuilder builder = new FixOrderCancelReplaceRequestBuilder(symbolMapper());
        OrderReplaceRequestedEvent event = new OrderReplaceRequestedEvent(
                ORDER_ID,
                /* originalQuantityScaled = */ 1_000_000_000L,
                /* originalLimitPriceScaledOrZero = */ 100_000_000L,
                /* newQuantityScaled = */ 1_500_000_000L,
                /* newLimitPriceScaledOrZero = */ 150_500_000L,
                /* requestedAtMillis = */ 1_700_000_000_000L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_GTC,
                "acct-1",
                "AAPL",
                "idem-rep-1",
                "operator-modify");

        OrderCancelReplaceRequest msg = builder.build(event);

        assertThat(msg.get(new OrigClOrdID()).getValue()).isEqualTo(ORDER_ID.toString());
        assertThat(msg.get(new ClOrdID()).getValue()).isEqualTo(ORDER_ID + ":r:idem-rep-1");
        assertThat(msg.get(new HandlInst()).getValue())
                .as("FIX 4.4 35=G requires HandlInst(21)")
                .isEqualTo(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION);
        assertThat(msg.get(new Symbol()).getValue()).isEqualTo("NASDAQ.AAPL");
        assertThat(msg.get(new Side()).getValue()).isEqualTo(Side.BUY);
        assertThat(msg.getString(OrderQty.FIELD))
                .as("OrderQty(38) on 35=G is the NEW TOTAL qty, not a delta")
                .isEqualTo("1.5");
        assertThat(msg.get(new OrdType()).getValue()).isEqualTo(OrdType.LIMIT);
        assertThat(msg.getString(Price.FIELD)).isEqualTo("150.5");
        assertThat(msg.get(new TimeInForce()).getValue()).isEqualTo(TimeInForce.GOOD_TILL_CANCEL);
    }

    @Test
    void replaceBuilder_qtyOnlyModify_copiesOriginalLimitPriceOntoTheWire() throws Exception {
        // newLimitPriceScaledOrZero = 0 means "keep existing limit price" — the builder must copy
        // the original price into the outbound 35=G because a 35=G with OrdType=LIMIT and no
        // Price(44) is malformed.
        FixOrderCancelReplaceRequestBuilder builder = new FixOrderCancelReplaceRequestBuilder(symbolMapper());
        OrderReplaceRequestedEvent event = new OrderReplaceRequestedEvent(
                ORDER_ID,
                1_000_000_000L,
                /* originalLimitPriceScaledOrZero = */ 200_000_000L,
                /* newQuantityScaled = */ 800_000_000L,
                /* newLimitPriceScaledOrZero = */ 0L,
                0L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-1",
                "AAPL",
                "idem-rep-qty",
                "");

        OrderCancelReplaceRequest msg = builder.build(event);

        assertThat(msg.get(new OrdType()).getValue()).isEqualTo(OrdType.LIMIT);
        assertThat(msg.getString(Price.FIELD))
                .as("qty-only modify on a limit order must echo the original limit price on the wire")
                .isEqualTo("200");
    }

    @Test
    void replaceBuilder_marketOrder_originalAndNewPriceBothZero_omitsPriceTag() throws Exception {
        FixOrderCancelReplaceRequestBuilder builder = new FixOrderCancelReplaceRequestBuilder(symbolMapper());
        OrderReplaceRequestedEvent event = new OrderReplaceRequestedEvent(
                ORDER_ID,
                1_000_000_000L,
                /* originalLimitPriceScaledOrZero = */ 0L,
                /* newQuantityScaled = */ 800_000_000L,
                /* newLimitPriceScaledOrZero = */ 0L,
                0L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-1",
                "AAPL",
                "idem-rep-mkt",
                "");

        OrderCancelReplaceRequest msg = builder.build(event);
        assertThat(msg.get(new OrdType()).getValue()).isEqualTo(OrdType.MARKET);
        assertThat(msg.isSetField(Price.FIELD))
                .as("MARKET 35=G must NOT carry Price(44)")
                .isFalse();
    }
}
