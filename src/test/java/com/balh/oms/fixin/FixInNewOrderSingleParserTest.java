package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.fix44.NewOrderSingle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixInNewOrderSingleParserTest {

    private FixInNewOrderSingleParser parser;

    @BeforeEach
    void setUp() {
        OmsConfig omsConfig = new OmsConfig();
        omsConfig.getFixIn().setSymbolMapJson("{\"tsla\":\"TSLA\"}");
        parser = new FixInNewOrderSingleParser(new FixInSymbolMapper(omsConfig, new ObjectMapper()), omsConfig);
    }

    @Test
    void parse_limitBuy_mapsFields() throws Exception {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID("ORD-1"));
        nos.set(new Symbol("tsla"));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.LIMIT));
        nos.set(new OrderQty(10));
        nos.set(new Price(150.25));
        nos.set(new TimeInForce(TimeInForce.DAY));

        FixInParsedNewOrder parsed = parser.parse(nos);

        assertThat(parsed.clientClOrdId()).isEqualTo("ORD-1");
        assertThat(parsed.instrumentSymbol()).isEqualTo("TSLA");
        assertThat(parsed.quantity()).isEqualByComparingTo("10");
        assertThat(parsed.limitPriceOrNull()).isEqualByComparingTo("150.25");
    }

    @Test
    void parse_portfolioIdTag_present_isCaptured() throws Exception {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID("ORD-PF"));
        nos.set(new Symbol("tsla"));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.MARKET));
        nos.set(new OrderQty(3));
        nos.set(new TimeInForce(TimeInForce.DAY));
        nos.setString(OmsConfig.FixIn.DEFAULT_PORTFOLIO_ID_TAG, "pf-pension");

        FixInParsedNewOrder parsed = parser.parse(nos);

        assertThat(parsed.portfolioIdOrNull()).isEqualTo("pf-pension");
    }

    @Test
    void parse_portfolioIdTag_absent_isNull() throws Exception {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID("ORD-NOPF"));
        nos.set(new Symbol("tsla"));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.MARKET));
        nos.set(new OrderQty(3));
        nos.set(new TimeInForce(TimeInForce.DAY));

        FixInParsedNewOrder parsed = parser.parse(nos);

        assertThat(parsed.portfolioIdOrNull()).isNull();
    }

    @Test
    void parse_limitWithoutPrice_throws() {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID("ORD-2"));
        nos.set(new Symbol("AAPL"));
        nos.set(new Side(Side.SELL));
        nos.set(new OrdType(OrdType.LIMIT));
        nos.set(new OrderQty(5));
        nos.set(new TimeInForce(TimeInForce.IMMEDIATE_OR_CANCEL));

        assertThatThrownBy(() -> parser.parse(nos))
                .isInstanceOf(FixInParseException.class)
                .hasMessage("limit_price_required");
    }
}
