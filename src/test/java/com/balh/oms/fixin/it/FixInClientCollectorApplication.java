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
import quickfix.field.RefMsgType;
import quickfix.field.SessionRejectReason;
import quickfix.field.Text;
import quickfix.fix44.BusinessMessageReject;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Reject;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test FIX-in client application that records inbound {@code ExecutionReport} and {@code BusinessMessageReject} messages. */
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
            return;
        }
        if (MsgType.REJECT.equals(msgType)) {
            recordSessionRejectIfPresent(message);
        }
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
