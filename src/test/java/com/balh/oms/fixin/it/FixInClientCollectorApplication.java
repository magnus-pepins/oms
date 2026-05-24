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
import quickfix.field.ExecType;
import quickfix.field.MsgType;
import quickfix.fix44.ExecutionReport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test FIX-in client application that records inbound {@code ExecutionReport} messages. */
public final class FixInClientCollectorApplication implements Application {

    public record ReceivedEr(String clOrdId, char execType, char ordStatus) {}

    public static final List<ReceivedEr> RECEIVED = new CopyOnWriteArrayList<>();

    public static void reset() {
        RECEIVED.clear();
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
            throws FieldNotFound, IncorrectTagValue, RejectLogon {}

    @Override
    public void toApp(Message message, SessionID sessionId) {}

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (!MsgType.EXECUTION_REPORT.equals(msgType)) {
            return;
        }
        ExecutionReport er = (ExecutionReport) message;
        RECEIVED.add(new ReceivedEr(
                er.getClOrdID().getValue(), er.getExecType().getValue(), er.getOrdStatus().getValue()));
        log.info(
                "FIX-in IT client ER clOrdId={} execType={} ordStatus={}",
                er.getClOrdID().getValue(),
                er.getExecType().getValue(),
                er.getOrdStatus().getValue());
    }
}
