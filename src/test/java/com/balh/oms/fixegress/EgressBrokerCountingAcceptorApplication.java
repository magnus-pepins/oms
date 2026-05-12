package com.balh.oms.fixegress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.MsgType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal QuickFIX/J acceptor {@link Application} for {@link OmsFixEgressBrokerIT}: counts
 * inbound {@code NewOrderSingle} and records the {@code ClOrdID} of each one.
 *
 * <p>Unlike {@code FixRoundTripAcceptorApplication}, this acceptor does <strong>not</strong>
 * synthesise an {@code ExecutionReport} reply. Slice 3b-2's IT validates the outbound NOS path
 * only (cluster → Aeron Archive replay → builder → {@code Session.sendToTarget}); inbound ER is
 * slice 3d's responsibility. Replying with an ER here would route through {@code OmsFixApplication}
 * → {@code FixInboundHandler} → {@code ExecutionReportApplier}, which writes Postgres rows that
 * presume an {@code orders} row exists — but in the egress JVM the projector is profile-mutex,
 * so no orders row materialises in this test. Counting + not replying keeps the IT scope tight.
 */
public final class EgressBrokerCountingAcceptorApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(EgressBrokerCountingAcceptorApplication.class);

    /**
     * Cumulative {@code NewOrderSingle} count across the test JVM. Reset by
     * {@link #resetItHooks()} before each scenario so assertions can compare against an
     * absolute count rather than a delta.
     */
    public static final AtomicInteger NOS_RECEIVED = new AtomicInteger(0);

    /** Cumulative ClOrdID record so the IT can assert exact-once delivery (no duplicates). */
    public static final List<UUID> CL_ORD_IDS_RECEIVED = new CopyOnWriteArrayList<>();

    public static void resetItHooks() {
        NOS_RECEIVED.set(0);
        CL_ORD_IDS_RECEIVED.clear();
    }

    @Override
    public void onCreate(SessionID sessionId) {}

    @Override
    public void onLogon(SessionID sessionId) {
        log.info("egress IT counting acceptor logon {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        log.info("egress IT counting acceptor logout {}", sessionId);
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
            // Slice 3b-2 always uses orderId.toString() as ClOrdID; record but don't crash.
            log.warn("egress IT counting acceptor: ClOrdID '{}' is not a UUID; counting anyway", clOrdId);
        }
        int n = NOS_RECEIVED.incrementAndGet();
        log.info("egress IT counting acceptor received NOS clOrdId={} (count={})", clOrdId, n);
    }
}
