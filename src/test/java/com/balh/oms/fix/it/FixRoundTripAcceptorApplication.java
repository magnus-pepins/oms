package com.balh.oms.fix.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrdStatus;
import quickfix.field.Price;
import quickfix.field.Symbol;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Minimal FIX 4.4 acceptor for slice-4 round-trip IT: reply to {@code D} with a synthetic full fill ER.
 */
public final class FixRoundTripAcceptorApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(FixRoundTripAcceptorApplication.class);

    /** IT hook: incremented for each inbound {@code D}; used by stale-outbound IT to assert no send. */
    public static final AtomicInteger NOS_RECEIVED = new AtomicInteger(0);

    /** IT hook: last {@code Symbol} on inbound {@code D} (symbol-map coverage). */
    public static final AtomicReference<String> LAST_NOS_SYMBOL = new AtomicReference<>();

    public static void resetItHooks() {
        NOS_RECEIVED.set(0);
        LAST_NOS_SYMBOL.set(null);
    }

    @Override
    public void onCreate(SessionID sessionId) {
        // no-op
    }

    @Override
    public void onLogon(SessionID sessionId) {
        log.info("FIX IT acceptor logon {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        log.info("FIX IT acceptor logout {}", sessionId);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
        // no-op
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound, IncorrectTagValue, RejectLogon {
        // no-op
    }

    @Override
    public void toApp(Message message, SessionID sessionId) {
        log.debug("FIX IT acceptor toApp {}", message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (!MsgType.ORDER_SINGLE.equals(msgType)) {
            throw new UnsupportedMessageType();
        }
        NOS_RECEIVED.incrementAndGet();
        String clOrdId = message.getString(ClOrdID.FIELD);
        String compact = clOrdId.replace("-", "");
        String sym = message.getString(Symbol.FIELD);
        LAST_NOS_SYMBOL.set(sym);
        String qtyStr = message.getString(OrderQty.FIELD);
        String pxStr = message.isSetField(Price.FIELD) ? message.getString(Price.FIELD) : "1";

        ExecutionReport er = new ExecutionReport();
        er.set(new OrderID("IT-VENUE-1"));
        er.set(new ExecID("it-exec-" + compact.substring(0, Math.min(12, compact.length()))));
        er.set(new ClOrdID(clOrdId));
        er.set(new Symbol(sym));
        er.set(new ExecType(ExecType.FILL));
        er.set(new OrdStatus(OrdStatus.FILLED));
        er.setString(OrderQty.FIELD, qtyStr);
        er.setString(LastQty.FIELD, qtyStr);
        er.setString(LeavesQty.FIELD, "0");
        er.setString(CumQty.FIELD, qtyStr);
        er.setString(LastPx.FIELD, pxStr);
        er.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));

        try {
            Session.sendToTarget(er, sessionId);
        } catch (quickfix.SessionNotFound e) {
            throw new IllegalStateException("FIX IT acceptor sendToTarget failed", e);
        }
        log.info("FIX IT acceptor sent synthetic fill for clOrdId={}", clOrdId);
    }
}
