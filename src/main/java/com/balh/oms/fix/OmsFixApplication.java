package com.balh.oms.fix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;

/**
 * QuickFIX/J {@link Application} adapter: logon registry + inbound app messages →
 * {@link FixInboundClusterSink}.
 *
 * <p>Phase 3 slice 3d of the Aeron Cluster substrate plan introduced
 * {@link FixInboundClusterSink} as the inbound applier; slice 3g deleted the legacy
 * {@code FixInboundHandler} + {@code FixInboundExecutionReportSink} interface, so this class now
 * routes directly to the cluster sink. The dispatch logic in {@link #fromApp} is unchanged.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class OmsFixApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(OmsFixApplication.class);

    private final FixSessionRegistry fixSessionRegistry;
    private final FixInboundClusterSink fixInboundSink;
    private final FixMassCancelOnDisconnectService massCancelOnDisconnectService;

    public OmsFixApplication(
            FixSessionRegistry fixSessionRegistry,
            FixInboundClusterSink fixInboundSink,
            FixMassCancelOnDisconnectService massCancelOnDisconnectService) {
        this.fixSessionRegistry = fixSessionRegistry;
        this.fixInboundSink = fixInboundSink;
        this.massCancelOnDisconnectService = massCancelOnDisconnectService;
    }

    @Override
    public void onCreate(SessionID sessionId) {
        log.debug("FIX session created {}", sessionId);
    }

    @Override
    public void onLogon(SessionID sessionId) {
        fixSessionRegistry.setActiveSession(sessionId);
        log.info("FIX logon {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        fixSessionRegistry.clear();
        massCancelOnDisconnectService.onInitiatorLogout(sessionId);
        log.info("FIX logout {}", sessionId);
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
        log.debug("FIX toApp {}", message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        try {
            if (MsgType.EXECUTION_REPORT.equals(msgType)) {
                fixInboundSink.handleExecutionReport(message);
            } else if (MsgType.ORDER_CANCEL_REJECT.equals(msgType)) {
                fixInboundSink.handleOrderCancelReject(message);
            } else {
                log.warn("Unsupported FIX app MsgType={} — {}", msgType, message);
                throw new UnsupportedMessageType();
            }
        } catch (FieldNotFound e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("FIX inbound handler failed for {}", message, e);
            throw e;
        }
    }
}
