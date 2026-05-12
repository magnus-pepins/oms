package com.balh.oms.fixegress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.UnsupportedMessageType;
import quickfix.field.AvgPx;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Slice 3d round-trip acceptor: counts inbound {@code NewOrderSingle} (mirrors
 * {@link EgressBrokerCountingAcceptorApplication}) and additionally synthesises a single
 * {@code ExecutionReport} {@code ExecType=PartialFill} reply per NOS, sent back to the OMS
 * initiator via {@link Session#sendToTarget(Message, SessionID)}.
 *
 * <p>The reply uses {@code lastQty = orderQty / 2} (a partial fill) at {@code lastPx = 1.00} so
 * the cluster's {@link com.balh.oms.cluster.OmsAdmissionClusteredService#applyExecutionReport}
 * walks to {@code STATUS_PARTIALLY_FILLED} and bumps the order version from 0 to 1. The IT
 * verifies the version bump round-trips by re-submitting the original {@code AcceptOrderCommand}
 * with the same {@code (accountId, clientIdempotencyKey)} pair — the cluster's idempotency
 * re-hit path returns the post-ER {@code AdmittedOrder.version}.
 *
 * <p><strong>Determinism is intentionally relaxed in the test acceptor.</strong> The reply uses
 * {@code Instant.now()} for the {@code TransactTime} and a short {@code ExecID} derived from a
 * counter; the FIX-egress JVM's {@code FixExecutionReportMapper} parses {@code TransactTime} into
 * an {@link java.time.Instant} (or substitutes {@link java.time.Instant#now} on parse failure)
 * before the cluster command is built — so {@code venueTsNanos} on the resulting
 * {@link com.balh.oms.cluster.ApplyExecutionReportCommand} comes from the FIX message, not from
 * the cluster's own clock.
 */
public final class EgressBrokerFillingAcceptorApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(EgressBrokerFillingAcceptorApplication.class);

    /** Cumulative {@code NewOrderSingle} count across the test JVM. Reset by {@link #resetItHooks}. */
    public static final AtomicInteger NOS_RECEIVED = new AtomicInteger(0);

    /** ClOrdIDs the acceptor has produced ER replies for. Useful for cross-test isolation. */
    public static final List<UUID> CL_ORD_IDS_RECEIVED = new CopyOnWriteArrayList<>();

    /** Monotonic counter used to keep ER ExecIDs unique across NOS replies in this JVM. */
    private static final AtomicInteger EXEC_ID_SEQ = new AtomicInteger(0);

    public static void resetItHooks() {
        NOS_RECEIVED.set(0);
        CL_ORD_IDS_RECEIVED.clear();
        EXEC_ID_SEQ.set(0);
    }

    @Override
    public void onCreate(SessionID sessionId) {}

    @Override
    public void onLogon(SessionID sessionId) {
        log.info("egress round-trip IT acceptor logon {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        log.info("egress round-trip IT acceptor logout {}", sessionId);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {}

    @Override
    public void fromAdmin(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, RejectLogon {}

    @Override
    public void toApp(Message message, SessionID sessionId) {}

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (!MsgType.ORDER_SINGLE.equals(msgType)) {
            throw new UnsupportedMessageType();
        }
        String clOrdId = message.getString(ClOrdID.FIELD);
        try {
            CL_ORD_IDS_RECEIVED.add(UUID.fromString(clOrdId));
        } catch (IllegalArgumentException e) {
            log.warn("egress round-trip IT acceptor: ClOrdID '{}' not a UUID; counting anyway", clOrdId);
        }
        int n = NOS_RECEIVED.incrementAndGet();
        log.info("egress round-trip IT acceptor received NOS clOrdId={} (count={})", clOrdId, n);

        try {
            ExecutionReport reply = buildPartialFillReply(message, clOrdId);
            Session.sendToTarget(reply, sessionId);
        } catch (FieldNotFound | SessionNotFound e) {
            log.error("egress round-trip IT acceptor failed to send ER reply for clOrdId={}", clOrdId, e);
        }
    }

    private static ExecutionReport buildPartialFillReply(Message nos, String clOrdId) throws FieldNotFound {
        BigDecimal orderQty = new BigDecimal(nos.getString(OrderQty.FIELD));
        BigDecimal lastQty = orderQty.divide(BigDecimal.valueOf(2L), 0, RoundingMode.DOWN);
        if (lastQty.signum() <= 0) {
            // OrderQty < 2 → fall back to a single-unit fill so PARTIAL_FILL is still half-progress.
            lastQty = BigDecimal.ONE;
        }
        BigDecimal cumQty = lastQty;
        BigDecimal leavesQty = orderQty.subtract(cumQty);
        BigDecimal lastPx = new BigDecimal("1.00");
        char side = nos.getChar(Side.FIELD);
        String symbol = nos.getString(Symbol.FIELD);

        ExecutionReport er = new ExecutionReport(
                new OrderID("BR-" + EXEC_ID_SEQ.incrementAndGet()),
                new ExecID("EXEC-" + EXEC_ID_SEQ.get()),
                new ExecType(ExecType.PARTIAL_FILL),
                new OrdStatus(OrdStatus.PARTIALLY_FILLED),
                new Side(side),
                new LeavesQty(leavesQty.doubleValue()),
                new CumQty(cumQty.doubleValue()),
                new AvgPx(lastPx.doubleValue()));
        er.set(new ClOrdID(clOrdId));
        er.set(new Symbol(symbol));
        er.set(new LastQty(lastQty.doubleValue()));
        er.set(new LastPx(lastPx.doubleValue()));
        er.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return er;
    }
}
