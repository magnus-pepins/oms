package com.balh.oms.fix.it;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.SessionID;
import quickfix.field.ClOrdID;
import quickfix.field.MsgType;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.fix44.NewOrderSingle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for bench-only behaviour in {@link FixRoundTripAcceptorApplication} (no embedded FIX
 * session — {@link quickfix.Session#sendToTarget} is not exercised).
 */
class FixRoundTripAcceptorApplicationTest {

    private static final SessionID SESSION =
            new SessionID("FIX.4.4", "IT-ACCEPTOR", "IT-INIT");

    private FixRoundTripAcceptorApplication app;

    @BeforeEach
    void setUp() {
        FixRoundTripAcceptorApplication.resetItHooks();
        app = new FixRoundTripAcceptorApplication();
    }

    @Test
    void sellNewOrderSingle_incrementsSellRejectCounter() throws Exception {
        Message nos = newOrderSingle(Side.SELL, "AAPL", OrdType.MARKET);

        assertThatThrownBy(() -> app.fromApp(nos, SESSION)).isInstanceOf(IllegalStateException.class);

        assertThat(FixRoundTripAcceptorApplication.SELL_REJECTS_SENT.get()).isEqualTo(1);
        assertThat(FixRoundTripAcceptorApplication.NOS_RECEIVED.get()).isEqualTo(1);
    }

    @Test
    void buyNewOrderSingle_doesNotIncrementSellRejectCounter() throws Exception {
        Message nos = newOrderSingle(Side.BUY, "AAPL", OrdType.MARKET);

        assertThatThrownBy(() -> app.fromApp(nos, SESSION)).isInstanceOf(IllegalStateException.class);

        assertThat(FixRoundTripAcceptorApplication.SELL_REJECTS_SENT.get()).isZero();
        assertThat(FixRoundTripAcceptorApplication.NOS_RECEIVED.get()).isEqualTo(1);
    }

    private static Message newOrderSingle(char side, String symbol, char ordType) throws FieldNotFound {
        NewOrderSingle nos = new NewOrderSingle();
        nos.getHeader().setField(new MsgType(MsgType.ORDER_SINGLE));
        nos.set(new ClOrdID("test-clord-" + System.nanoTime()));
        nos.set(new Symbol(symbol));
        nos.set(new Side(side));
        nos.set(new OrderQty(1));
        nos.set(new OrdType(ordType));
        return nos;
    }
}
