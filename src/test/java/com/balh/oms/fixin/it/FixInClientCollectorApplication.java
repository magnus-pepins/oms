package com.balh.oms.fixin.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;
import quickfix.fix44.BusinessMessageReject;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Reject;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test FIX-in client application that records inbound {@link ExecutionReport},
 * {@link BusinessMessageReject}, and session-level {@link Reject} messages.
 *
 * <h2>Concurrency contract</h2>
 *
 * The three collector lists ({@link #RECEIVED}, {@link #BUSINESS_REJECTS},
 * {@link #SESSION_REJECTS}) are {@code static} and use {@link CopyOnWriteArrayList} so concurrent
 * writes from the QuickFIX session thread and concurrent reads from the assertion thread are safe.
 *
 * However, the contents are shared across every collector instance in the JVM. Probes that run
 * multiple scenarios must call {@link #reset()} between scenarios. Probes (or integration tests)
 * that need parallel isolation must run in separate JVMs. The bundled
 * {@code FixInLoopbackConformanceProbeMain} runs scenarios sequentially in a single JVM and resets
 * between each.
 */
public final class FixInClientCollectorApplication implements Application {

    public record ReceivedEr(String clOrdId, char execType, char ordStatus, String orderIdOrNull) {}

    public record ReceivedBmr(String refMsgType, String text) {}

    public record ReceivedReject(String refMsgType, String text, int sessionRejectReason) {}

    public static final List<ReceivedEr> RECEIVED = new CopyOnWriteArrayList<>();
    public static final List<ReceivedBmr> BUSINESS_REJECTS = new CopyOnWriteArrayList<>();
    public static final List<ReceivedReject> SESSION_REJECTS = new CopyOnWriteArrayList<>();

    public static void reset() {
        RECEIVED.clear();
        BUSINESS_REJECTS.clear();
        SESSION_REJECTS.clear();
    }

    private static final Logger log = LoggerFactory.getLogger(FixInClientCollectorApplication.class);

    @Override
    public void onCreate(SessionID sessionId) {}

    @Override
    public void onLogon(SessionID sessionId) {
        log.info("FIX-in IT client logon {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        log.info("FIX-in IT client logout {}", sessionId);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {}

    @Override
    public void fromAdmin(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, RejectLogon {
        recordSessionRejectIfPresent(message);
    }

    @Override
    public void toApp(Message message, SessionID sessionId) {}

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (MsgType.EXECUTION_REPORT.equals(msgType)) {
            ExecutionReport er = (ExecutionReport) message;
            String orderId = er.isSetOrderID() ? er.getOrderID().getValue() : null;
            RECEIVED.add(new ReceivedEr(
                    er.getClOrdID().getValue(),
                    er.getExecType().getValue(),
                    er.getOrdStatus().getValue(),
                    orderId));
            log.info(
                    "FIX-in IT client ER clOrdId={} execType={} ordStatus={} orderId={}",
                    er.getClOrdID().getValue(),
                    er.getExecType().getValue(),
                    er.getOrdStatus().getValue(),
                    orderId);
            return;
        }
        if (MsgType.BUSINESS_MESSAGE_REJECT.equals(msgType)) {
            BusinessMessageReject bmr = (BusinessMessageReject) message;
            String refMsgType = bmr.isSetRefMsgType() ? bmr.getRefMsgType().getValue() : "";
            String text = bmr.isSetText() ? bmr.getText().getValue() : "";
            BUSINESS_REJECTS.add(new ReceivedBmr(refMsgType, text));
            log.info("FIX-in IT client BMR refMsgType={} text={}", refMsgType, text);
        }
        // Session-level Reject (35=3) is routed to fromAdmin by QuickFIX; do not duplicate-handle here.
    }

    private static void recordSessionRejectIfPresent(Message message) throws FieldNotFound {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (!MsgType.REJECT.equals(msgType)) {
            return;
        }
        Reject reject = (Reject) message;
        String refMsgType = reject.isSetRefMsgType() ? reject.getRefMsgType().getValue() : "";
        String text = reject.isSetText() ? reject.getText().getValue() : "";
        int reason = reject.isSetSessionRejectReason() ? reject.getSessionRejectReason().getValue() : -1;
        SESSION_REJECTS.add(new ReceivedReject(refMsgType, text, reason));
        log.info("FIX-in IT client Reject refMsgType={} text={} reason={}", refMsgType, text, reason);
    }
}
