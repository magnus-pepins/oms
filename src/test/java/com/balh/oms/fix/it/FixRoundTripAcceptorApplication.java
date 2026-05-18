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
import quickfix.field.CxlRejReason;
import quickfix.field.CxlRejResponseTo;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrigClOrdID;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.OrderCancelReject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FIX 4.4 acceptor for the full-lifecycle round-trip + the existing slice-4 ITs.
 *
 * <h2>Behaviour by inbound message, {@code OrdType} (tag 40), and {@code Symbol} trigger</h2>
 *
 * <p>Two orthogonal signals control the synthesised reply: the standard FIX {@code OrdType}
 * (which mirrors what a real venue uses to decide if an order can fill immediately) and a
 * {@code Symbol}-based trigger for deterministic edge-case scenarios that don't have a clean
 * FIX-native trigger (split fills, cancel/replace rejects).
 *
 * <table>
 *   <caption>NewOrderSingle (35=D) reply by OrdType + Symbol trigger</caption>
 *   <tr><th>Symbol = "PARTIAL"</th>
 *       <th>OrdType = MARKET (40=1)</th>
 *       <th>OrdType = LIMIT (40=2)</th></tr>
 *   <tr><td>partial fill (ER ET=1) then full fill (ER ET=2)</td>
 *       <td>full fill (ER ET=2)</td>
 *       <td>NEW ack (ER ET=0) — order rests in book, no fill until 35=F/35=G</td></tr>
 * </table>
 *
 * <table>
 *   <caption>Cancel/replace (35=F/35=G) reply by Symbol trigger</caption>
 *   <tr><th>Inbound</th><th>Symbol = "REJECT"</th><th>Other</th></tr>
 *   <tr><td>35=F OrderCancelRequest</td>
 *       <td>OrderCancelReject 35=9 (CxlRejReason=1 unknown-order)</td>
 *       <td>cancel ack (ER ET=4)</td></tr>
 *   <tr><td>35=G OrderCancelReplaceRequest</td>
 *       <td>OrderCancelReject 35=9 (CxlRejResponseTo=2)</td>
 *       <td>replace ack (ER ET=5)</td></tr>
 * </table>
 *
 * <p>The {@code OrdType=LIMIT} branch lets the operator see live ACCEPTED → WORKING states
 * persist on the desk and customer screens (the order sits open at the venue) and then
 * deliberately progress the lifecycle by cancelling or modifying. This mirrors how Alpaca
 * paper, IBKR Paper, and Bloomberg EMSX test environments behave: market orders cross
 * instantly, limit orders rest in the book until manually acted on or until a market move
 * triggers a fill. Production venues do the same. This is not a demo hack — same path runs in
 * CI for any future "LIMIT order lifecycle" integration tests.
 *
 * <p>For the partial-then-full split the first ER carries {@code LastQty = OrderQty / 2,
 * CumQty = OrderQty / 2, LeavesQty = OrderQty / 2, OrdStatus=1 PARTIALLY_FILLED}; the second
 * ER carries the remainder with {@code OrdStatus=2 FILLED}. Both ERs share the same
 * {@code ClOrdID}, which is how OMS's
 * {@link com.balh.oms.fix.FixInboundClusterSink} dedupes them on
 * {@code (orderId, venueExecRef)}.
 *
 * <p>For the replace ack we echo the {@code OrderQty} and {@code Price} from the inbound 35=G
 * back to OMS so the cluster state machine can update its in-memory order with the new
 * quantity/price. The replace ER carries {@code CumQty = 0, LeavesQty = newOrderQty,
 * OrdStatus=0 NEW} — same convention as the QuickFIX banzai sample acceptor.
 *
 * <p>The original slice-4 ITs that just check NOS → fill continue to work unchanged because
 * those ITs send MARKET orders (no Price field), which still hit the instant-fill branch.
 */
public final class FixRoundTripAcceptorApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(FixRoundTripAcceptorApplication.class);

    /** Symbol that triggers a partial-then-full split on a 35=D. */
    public static final String TRIGGER_SYMBOL_PARTIAL = "PARTIAL";

    /**
     * Symbol that triggers a 35=9 OrderCancelReject on a 35=F or 35=G. Picked to be loud and
     * obvious on the demo screen ("cancel REJECT order"). The matching customer-frontend demo
     * fixture seeds an order against this symbol so the cancel-rejected path is one click away.
     */
    public static final String TRIGGER_SYMBOL_REJECT = "REJECT";

    /** IT hook: incremented for each inbound {@code D}; used by stale-outbound IT to assert no send. */
    public static final AtomicInteger NOS_RECEIVED = new AtomicInteger(0);

    /** IT hook: last {@code Symbol} on inbound {@code D} (symbol-map coverage). */
    public static final AtomicReference<String> LAST_NOS_SYMBOL = new AtomicReference<>();

    /** IT hook: incremented for each inbound 35=F handled. */
    public static final AtomicInteger CANCEL_REQUESTS_RECEIVED = new AtomicInteger(0);

    /** IT hook: incremented for each inbound 35=G handled. */
    public static final AtomicInteger REPLACE_REQUESTS_RECEIVED = new AtomicInteger(0);

    /** IT hook: incremented for each 35=9 OrderCancelReject the acceptor sent back. */
    public static final AtomicInteger CANCEL_REJECTS_SENT = new AtomicInteger(0);

    public static void resetItHooks() {
        NOS_RECEIVED.set(0);
        LAST_NOS_SYMBOL.set(null);
        CANCEL_REQUESTS_RECEIVED.set(0);
        REPLACE_REQUESTS_RECEIVED.set(0);
        CANCEL_REJECTS_SENT.set(0);
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
        switch (msgType) {
            case MsgType.ORDER_SINGLE -> onNewOrderSingle(message, sessionId);
            case MsgType.ORDER_CANCEL_REQUEST -> onOrderCancelRequest(message, sessionId);
            case MsgType.ORDER_CANCEL_REPLACE_REQUEST -> onOrderCancelReplaceRequest(message, sessionId);
            default -> throw new UnsupportedMessageType();
        }
    }

    // -----------------------------------------------------------------------
    // 35=D NewOrderSingle  →  ER ET=2 FILL  (or PARTIAL then FILL when Symbol=PARTIAL)
    // -----------------------------------------------------------------------

    private void onNewOrderSingle(Message message, SessionID sessionId) throws FieldNotFound {
        NOS_RECEIVED.incrementAndGet();
        String clOrdId = message.getString(ClOrdID.FIELD);
        String sym = message.getString(Symbol.FIELD);
        LAST_NOS_SYMBOL.set(sym);
        BigDecimal orderQty = new BigDecimal(message.getString(OrderQty.FIELD));
        String px = message.isSetField(Price.FIELD) ? message.getString(Price.FIELD) : "1";
        // OMS always sets tag 40; default to MARKET defensively for older slice-4 tests that
        // build NOS by hand without OrdType.
        char ordType = message.isSetField(OrdType.FIELD) ? message.getChar(OrdType.FIELD) : OrdType.MARKET;

        if (TRIGGER_SYMBOL_PARTIAL.equals(sym)) {
            // Two ERs sharing the same ClOrdID. ExecID is distinct per ER (OMS dedupes on
            // (orderId, venueExecRef); same ExecID would collapse the second ER on the wire).
            BigDecimal halfQty = orderQty.divide(BigDecimal.valueOf(2), 0, RoundingMode.DOWN);
            BigDecimal remaining = orderQty.subtract(halfQty);
            sendExecutionReport(sessionId, clOrdId, sym, px,
                    /* lastQty   = */ halfQty.toPlainString(),
                    /* cumQty    = */ halfQty.toPlainString(),
                    /* leavesQty = */ remaining.toPlainString(),
                    ExecType.PARTIAL_FILL,
                    OrdStatus.PARTIALLY_FILLED,
                    /* execIdSuffix = */ "p1");
            sendExecutionReport(sessionId, clOrdId, sym, px,
                    /* lastQty   = */ remaining.toPlainString(),
                    /* cumQty    = */ orderQty.toPlainString(),
                    /* leavesQty = */ "0",
                    ExecType.FILL,
                    OrdStatus.FILLED,
                    /* execIdSuffix = */ "p2");
            log.info("FIX IT acceptor sent partial-then-full pair for clOrdId={}", clOrdId);
            return;
        }

        if (ordType == OrdType.LIMIT) {
            // LIMIT orders rest in the venue book — synthesise an ER ET=0 NEW so OMS sees an
            // explicit venue ack and walks status to WORKING. No fill is sent: the order sits
            // open at the (synthetic) venue until a 35=F or 35=G arrives. This is the standard
            // behaviour real venues exhibit when a limit price doesn't cross the inside market.
            sendExecutionReport(sessionId, clOrdId, sym, px,
                    /* lastQty   = */ "0",
                    /* cumQty    = */ "0",
                    /* leavesQty = */ orderQty.toPlainString(),
                    ExecType.NEW,
                    OrdStatus.NEW,
                    /* execIdSuffix = */ "ack");
            log.info(
                    "FIX IT acceptor sent NEW ack for LIMIT clOrdId={} px={} qty={} — order rests in book until cancel/replace",
                    clOrdId,
                    px,
                    orderQty.toPlainString());
            return;
        }

        sendExecutionReport(sessionId, clOrdId, sym, px,
                /* lastQty   = */ orderQty.toPlainString(),
                /* cumQty    = */ orderQty.toPlainString(),
                /* leavesQty = */ "0",
                ExecType.FILL,
                OrdStatus.FILLED,
                /* execIdSuffix = */ "f");
        log.info("FIX IT acceptor sent synthetic fill for MARKET clOrdId={}", clOrdId);
    }

    // -----------------------------------------------------------------------
    // 35=F OrderCancelRequest  →  ER ET=4 CANCELED  (or 35=9 when Symbol=REJECT)
    // -----------------------------------------------------------------------

    private void onOrderCancelRequest(Message message, SessionID sessionId) throws FieldNotFound {
        CANCEL_REQUESTS_RECEIVED.incrementAndGet();
        String clOrdId = message.getString(ClOrdID.FIELD);
        String origClOrdId = message.getString(OrigClOrdID.FIELD);
        String sym = message.isSetField(Symbol.FIELD) ? message.getString(Symbol.FIELD) : "";
        String orderQty = message.isSetField(OrderQty.FIELD) ? message.getString(OrderQty.FIELD) : "0";

        if (TRIGGER_SYMBOL_REJECT.equals(sym)) {
            sendOrderCancelReject(sessionId, clOrdId, origClOrdId,
                    CxlRejResponseTo.ORDER_CANCEL_REQUEST,
                    CxlRejReason.UNKNOWN_ORDER,
                    "REJECT trigger symbol — demo cancel-reject");
            log.info("FIX IT acceptor sent OrderCancelReject for clOrdId={} (origClOrdId={}) — REJECT trigger",
                    clOrdId, origClOrdId);
            return;
        }

        // Cancel ack: ET=4 CANCELED, OrdStatus=4. CumQty echoes back from the original (the
        // broker would track it; for the loopback we just send 0 — the cluster's state machine
        // is the authority on cumQty already).
        sendExecutionReport(sessionId, origClOrdId, sym, "0",
                /* lastQty   = */ "0",
                /* cumQty    = */ "0",
                /* leavesQty = */ "0",
                ExecType.CANCELED,
                OrdStatus.CANCELED,
                /* execIdSuffix = */ "c");
        log.info("FIX IT acceptor sent cancel ack for origClOrdId={} qty={}", origClOrdId, orderQty);
    }

    // -----------------------------------------------------------------------
    // 35=G OrderCancelReplaceRequest  →  ER ET=5 REPLACE  (or 35=9 when Symbol=REJECT)
    // -----------------------------------------------------------------------

    private void onOrderCancelReplaceRequest(Message message, SessionID sessionId) throws FieldNotFound {
        REPLACE_REQUESTS_RECEIVED.incrementAndGet();
        String clOrdId = message.getString(ClOrdID.FIELD);
        String origClOrdId = message.getString(OrigClOrdID.FIELD);
        String sym = message.isSetField(Symbol.FIELD) ? message.getString(Symbol.FIELD) : "";
        String newOrderQty = message.getString(OrderQty.FIELD);
        String newPx = message.isSetField(Price.FIELD) ? message.getString(Price.FIELD) : "1";

        if (TRIGGER_SYMBOL_REJECT.equals(sym)) {
            sendOrderCancelReject(sessionId, clOrdId, origClOrdId,
                    CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST,
                    CxlRejReason.UNKNOWN_ORDER,
                    "REJECT trigger symbol — demo replace-reject");
            log.info("FIX IT acceptor sent OrderCancelReject for replace clOrdId={} (origClOrdId={}) — REJECT trigger",
                    clOrdId, origClOrdId);
            return;
        }

        // Replace ack: ET=5 REPLACE, OrdStatus=0 NEW (no fill happened, the order is alive at
        // the new qty/price). LeavesQty = new OrderQty, CumQty = 0. OMS's cluster state machine
        // applies the new qty/price + bumps version. Subsequent fills land on the replaced
        // contract.
        sendExecutionReport(sessionId, origClOrdId, sym, newPx,
                /* lastQty   = */ "0",
                /* cumQty    = */ "0",
                /* leavesQty = */ newOrderQty,
                ExecType.REPLACED,
                OrdStatus.NEW,
                /* execIdSuffix = */ "r",
                /* overrideOrderQty = */ newOrderQty);
        log.info("FIX IT acceptor sent replace ack for origClOrdId={} newQty={} newPx={}",
                origClOrdId, newOrderQty, newPx);
    }

    // -----------------------------------------------------------------------
    // Common ER + OCR senders
    // -----------------------------------------------------------------------

    private void sendExecutionReport(
            SessionID sessionId,
            String clOrdId,
            String sym,
            String px,
            String lastQty,
            String cumQty,
            String leavesQty,
            char execType,
            char ordStatus,
            String execIdSuffix) {
        sendExecutionReport(sessionId, clOrdId, sym, px, lastQty, cumQty, leavesQty,
                execType, ordStatus, execIdSuffix, /* overrideOrderQty = */ null);
    }

    private void sendExecutionReport(
            SessionID sessionId,
            String clOrdId,
            String sym,
            String px,
            String lastQty,
            String cumQty,
            String leavesQty,
            char execType,
            char ordStatus,
            String execIdSuffix,
            String overrideOrderQty) {
        String compact = clOrdId.replace("-", "");
        String execIdCore = compact.substring(0, Math.min(12, compact.length()));
        ExecutionReport er = new ExecutionReport();
        er.set(new OrderID("IT-VENUE-1"));
        er.set(new ExecID("it-exec-" + execIdCore + "-" + execIdSuffix));
        er.set(new ClOrdID(clOrdId));
        er.set(new Symbol(sym));
        er.set(new ExecType(execType));
        er.set(new OrdStatus(ordStatus));
        // Side is required by FIX 4.4 schema even on replies; we echo BUY because every demo
        // path is BUY-side. Production-grade acceptors would echo the original NOS's Side from
        // an in-memory order book.
        er.set(new Side(Side.BUY));
        er.setString(OrderQty.FIELD, overrideOrderQty != null ? overrideOrderQty : addOrZero(cumQty, leavesQty));
        er.setString(LastQty.FIELD, lastQty);
        er.setString(LeavesQty.FIELD, leavesQty);
        er.setString(CumQty.FIELD, cumQty);
        er.setString(LastPx.FIELD, px);
        er.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));

        try {
            Session.sendToTarget(er, sessionId);
        } catch (quickfix.SessionNotFound e) {
            throw new IllegalStateException("FIX IT acceptor ER sendToTarget failed", e);
        }
    }

    private void sendOrderCancelReject(
            SessionID sessionId,
            String clOrdId,
            String origClOrdId,
            char cxlRejResponseTo,
            int cxlRejReason,
            String text) {
        CANCEL_REJECTS_SENT.incrementAndGet();
        OrderCancelReject ocr = new OrderCancelReject();
        ocr.set(new OrderID("IT-VENUE-1"));
        ocr.set(new ClOrdID(clOrdId));
        ocr.set(new OrigClOrdID(origClOrdId));
        // OrdStatus on a 35=9 reflects the broker's view of the order at this moment. We send
        // NEW (0) for the demo trigger; production brokers would echo their own bookkeeping.
        ocr.set(new OrdStatus(OrdStatus.NEW));
        ocr.set(new CxlRejResponseTo(cxlRejResponseTo));
        ocr.set(new CxlRejReason(cxlRejReason));
        ocr.set(new Text(text));

        try {
            Session.sendToTarget(ocr, sessionId);
        } catch (quickfix.SessionNotFound e) {
            throw new IllegalStateException("FIX IT acceptor OCR sendToTarget failed", e);
        }
    }

    private static String addOrZero(String a, String b) {
        try {
            return new BigDecimal(a).add(new BigDecimal(b)).toPlainString();
        } catch (NumberFormatException e) {
            return "0";
        }
    }
}
