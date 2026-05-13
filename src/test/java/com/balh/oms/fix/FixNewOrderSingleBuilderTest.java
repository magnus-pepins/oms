package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.fix44.NewOrderSingle;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit coverage for {@link FixNewOrderSingleBuilder#build(OrderAdmittedEvent)}, the Phase 3 slice
 * 3b-2 NOS-from-cluster-event path used by {@code OmsFixEgressService}. This path bypasses the
 * Postgres lookup that {@link FixNewOrderSingleBuilder#build(com.balh.oms.domain.Order)} uses, so
 * the hot send path on the egress JVM is {@code AeronArchive.replay} → builder → QuickFIX
 * {@code Session.sendToTarget}. Coverage guards the field mapping (clOrdId, side, qty/price
 * scaling, OrdType market vs limit, TIF) and the symbol-mapper handoff.
 */
class FixNewOrderSingleBuilderTest {

    private static final UUID ORDER_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

    private FixNewOrderSingleBuilder newBuilder(String symbolMapJson) {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setSymbolMapJson(symbolMapJson);
        FixSymbolMapper symbolMapper = new FixSymbolMapper(cfg, new ObjectMapper());
        return new FixNewOrderSingleBuilder(symbolMapper);
    }

    private OrderAdmittedEvent admittedEvent(
            byte side, byte tif, long qtyScaled, long priceScaledOrZero, String instrumentSymbol) {
        return new OrderAdmittedEvent(
                ORDER_ID,
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ 0L,
                qtyScaled,
                priceScaledOrZero,
                /* shardId = */ 0,
                /* version = */ 1,
                side,
                tif,
                /* accountId = */ "00000000-0000-0000-0000-000000000001",
                /* clientIdempotencyKey = */ "idem-key",
                /* accountIdHash = */ "hash",
                instrumentSymbol,
                /* ledgerBalanceIdOrNull = */ null);
    }

    @Test
    void build_buy_limit_day_mapsAllNosFields() throws Exception {
        FixNewOrderSingleBuilder builder = newBuilder("{}");
        // 10 units at 5.00, BUY, DAY, AAPL.
        OrderAdmittedEvent ev = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                /* qty = 10 */ 10L * AcceptOrderCommand.QUANTITY_SCALE,
                /* px  = 5 */ 5L * AcceptOrderCommand.PRICE_SCALE,
                "AAPL");

        NewOrderSingle nos = builder.build(ev);

        assertThat(nos.getString(ClOrdID.FIELD)).isEqualTo(ORDER_ID.toString());
        assertThat(nos.getString(Symbol.FIELD)).isEqualTo("AAPL");
        assertThat(nos.getChar(Side.FIELD)).isEqualTo(Side.BUY);
        assertThat(nos.getString(OrderQty.FIELD)).isEqualTo("10");
        assertThat(nos.getChar(OrdType.FIELD)).isEqualTo(OrdType.LIMIT);
        assertThat(nos.getString(Price.FIELD)).isEqualTo("5");
        assertThat(nos.getChar(TimeInForce.FIELD)).isEqualTo(TimeInForce.DAY);
    }

    @Test
    void build_sell_market_ioc_omitsPriceAndUsesMarketOrdType() throws Exception {
        FixNewOrderSingleBuilder builder = newBuilder("{}");
        // 2.5 units at MARKET, SELL, IOC.
        OrderAdmittedEvent ev = admittedEvent(
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_IOC,
                /* qty = 2.5 */ 2_500_000_000L,
                /* market */ 0L,
                "MSFT");

        NewOrderSingle nos = builder.build(ev);

        assertThat(nos.getChar(Side.FIELD)).isEqualTo(Side.SELL);
        assertThat(nos.getString(OrderQty.FIELD)).isEqualTo("2.5");
        assertThat(nos.getChar(OrdType.FIELD)).isEqualTo(OrdType.MARKET);
        assertThat(nos.isSetField(Price.FIELD)).isFalse();
        assertThat(nos.getChar(TimeInForce.FIELD)).isEqualTo(TimeInForce.IMMEDIATE_OR_CANCEL);
    }

    @Test
    void build_fok_gtc_mapToFixCharCodes() throws Exception {
        FixNewOrderSingleBuilder builder = newBuilder("{}");

        OrderAdmittedEvent fok = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_FOK,
                AcceptOrderCommand.QUANTITY_SCALE,
                AcceptOrderCommand.PRICE_SCALE,
                "AAPL");
        assertThat(builder.build(fok).getChar(TimeInForce.FIELD)).isEqualTo(TimeInForce.FILL_OR_KILL);

        OrderAdmittedEvent gtc = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_GTC,
                AcceptOrderCommand.QUANTITY_SCALE,
                AcceptOrderCommand.PRICE_SCALE,
                "AAPL");
        assertThat(builder.build(gtc).getChar(TimeInForce.FIELD)).isEqualTo(TimeInForce.GOOD_TILL_CANCEL);
    }

    @Test
    void build_routesThroughSymbolMapper_caseInsensitive() throws Exception {
        // OMS instrument "AAPL" -> broker symbol "AAPL.OQ".
        FixNewOrderSingleBuilder builder = newBuilder("{\"aapl\":\"AAPL.OQ\"}");
        OrderAdmittedEvent ev = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                AcceptOrderCommand.QUANTITY_SCALE,
                AcceptOrderCommand.PRICE_SCALE,
                "AAPL");

        NewOrderSingle nos = builder.build(ev);

        assertThat(nos.getString(Symbol.FIELD)).isEqualTo("AAPL.OQ");
    }

    @Test
    void build_fractionalQuantityAndPrice_preservesFixedPointPrecision() throws Exception {
        FixNewOrderSingleBuilder builder = newBuilder("{}");
        // qty = 0.123456789 (full 1e9 precision), px = 1.234567 (full 1e6 precision).
        OrderAdmittedEvent ev = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                /* 0.123456789 */ 123_456_789L,
                /* 1.234567   */ 1_234_567L,
                "AAPL");

        NewOrderSingle nos = builder.build(ev);

        assertThat(nos.getString(OrderQty.FIELD)).isEqualTo("0.123456789");
        assertThat(nos.getString(Price.FIELD)).isEqualTo("1.234567");
    }

    @Test
    void build_unknownSideCode_throws() {
        FixNewOrderSingleBuilder builder = newBuilder("{}");
        OrderAdmittedEvent ev = admittedEvent(
                /* invalid side */ (byte) 99,
                AcceptOrderCommand.TIF_DAY,
                AcceptOrderCommand.QUANTITY_SCALE,
                AcceptOrderCommand.PRICE_SCALE,
                "AAPL");
        assertThatThrownBy(() -> builder.build(ev))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown OrderAdmittedEvent.side");
    }

    @Test
    void build_unknownTifCode_throws() {
        FixNewOrderSingleBuilder builder = newBuilder("{}");
        OrderAdmittedEvent ev = admittedEvent(
                AcceptOrderCommand.SIDE_BUY,
                /* invalid tif */ (byte) 99,
                AcceptOrderCommand.QUANTITY_SCALE,
                AcceptOrderCommand.PRICE_SCALE,
                "AAPL");
        assertThatThrownBy(() -> builder.build(ev))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown OrderAdmittedEvent.timeInForceCode");
    }
}
