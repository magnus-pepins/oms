package com.balh.oms.fixin;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.FieldNotFound;
import quickfix.Session;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.MsgType;
import quickfix.field.Password;
import quickfix.field.SendingTime;
import quickfix.field.Username;
import quickfix.fix44.BusinessMessageReject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInApplication implements Application {

    private static final Logger log = LoggerFactory.getLogger(FixInApplication.class);

    private final com.balh.oms.config.OmsConfig omsConfig;
    private final FixInSessionRepository sessionRepository;
    private final FixInSessionRegistry sessionRegistry;
    private final FixInIngressService ingressService;
    private final FixInAppMessageRateLimiter rateLimiter;
    private final FixInOpsMetrics metrics;

    private static final DateTimeFormatter FIX_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss");

    public FixInApplication(
            com.balh.oms.config.OmsConfig omsConfig,
            FixInSessionRepository sessionRepository,
            FixInSessionRegistry sessionRegistry,
            FixInIngressService ingressService,
            FixInAppMessageRateLimiter rateLimiter,
            FixInOpsMetrics metrics) {
        this.omsConfig = omsConfig;
        this.sessionRepository = sessionRepository;
        this.sessionRegistry = sessionRegistry;
        this.ingressService = ingressService;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    @Override
    public void onCreate(SessionID sessionId) {
        log.debug("FIX-in session created {}", sessionId);
    }

    @Override
    public void onLogon(SessionID sessionId) {
        Optional<FixInSessionRow> row = sessionRepository.findByWireCompIds(
                sessionId.getTargetCompID(), sessionId.getSenderCompID(), sessionId.getSessionQualifier());
        row.ifPresentOrElse(
                session -> {
                    if (!session.enabled()) {
                        log.warn("FIX-in logon rejected — session disabled {}", sessionId);
                        return;
                    }
                    sessionRegistry.register(sessionId, session);
                    log.info("FIX-in logon {} → sessionId={}", sessionId, session.id());
                },
                () -> log.warn("FIX-in logon for unknown comp ids {}", sessionId));
    }

    @Override
    public void onLogout(SessionID sessionId) {
        sessionRegistry.unregister(sessionId);
        log.info("FIX-in logout {}", sessionId);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
        // no-op
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, RejectLogon {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (!MsgType.LOGON.equals(msgType)) {
            return;
        }
        Optional<FixInSessionRow> row = sessionRepository.findByWireCompIds(
                sessionId.getTargetCompID(), sessionId.getSenderCompID(), sessionId.getSessionQualifier());
        if (row.isEmpty() || !row.get().enabled()) {
            throw new RejectLogon("unknown_session");
        }
        if (!omsConfig.getFixIn().isRequireLogonCredentials()) {
            return;
        }
        FixInSessionRow session = row.get();
        if (session.logonUsernameOrNull() == null || session.passwordHashOrNull() == null) {
            return;
        }
        String username = message.isSetField(Username.FIELD) ? message.getString(Username.FIELD) : "";
        String password = message.isSetField(Password.FIELD) ? message.getString(Password.FIELD) : "";
        if (!session.logonUsernameOrNull().equals(username) || !session.passwordHashOrNull().equals(password)) {
            throw new RejectLogon("invalid_credentials");
        }
    }

    @Override
    public void toApp(Message message, SessionID sessionId) {
        log.debug("FIX-in toApp {}", message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        if (!rateLimiter.tryAcquire(sessionId)) {
            metrics.rateLimitRejected();
            rejectAppMessage(sessionId, message, "rate_limit_exceeded");
            return;
        }
        if (!withinMessageAge(message)) {
            rejectAppMessage(sessionId, message, "message_too_old");
            return;
        }
        if (message.isSetField(ClOrdID.FIELD)) {
            String clOrdId = message.getString(ClOrdID.FIELD);
            if (clOrdId.length() > omsConfig.getFixIn().getMaxClientClOrdIdLength()) {
                rejectAppMessage(sessionId, message, "cl_ord_id_too_long");
                return;
            }
        }
        Optional<FixInSessionRow> sessionOpt = sessionRegistry.find(sessionId);
        if (sessionOpt.isPresent() && "DROP_COPY".equalsIgnoreCase(sessionOpt.get().sessionMode())) {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.ORDER_SINGLE.equals(msgType)
                    || MsgType.ORDER_CANCEL_REQUEST.equals(msgType)
                    || MsgType.ORDER_CANCEL_REPLACE_REQUEST.equals(msgType)) {
                ingressService.rejectDropCopyOrderEntry(message, sessionId);
                return;
            }
            throw new UnsupportedMessageType();
        }

        String msgType = message.getHeader().getString(MsgType.FIELD);
        if (MsgType.ORDER_SINGLE.equals(msgType)) {
            ingressService.handleNewOrderSingle(message, sessionId);
            return;
        }
        if (MsgType.ORDER_CANCEL_REQUEST.equals(msgType)) {
            ingressService.handleOrderCancelRequest(message, sessionId);
            return;
        }
        if (MsgType.ORDER_CANCEL_REPLACE_REQUEST.equals(msgType)) {
            ingressService.handleOrderCancelReplaceRequest(message, sessionId);
            return;
        }
        log.warn("Unsupported FIX-in app MsgType={} — {}", msgType, message);
        throw new UnsupportedMessageType();
    }

    private boolean withinMessageAge(Message message) throws FieldNotFound {
        long maxAgeMs = omsConfig.getFixIn().getMaxMessageAgeMs();
        if (maxAgeMs <= 0 || !message.getHeader().isSetField(SendingTime.FIELD)) {
            return true;
        }
        String sendingTime = message.getHeader().getString(SendingTime.FIELD);
        LocalDateTime sent = LocalDateTime.parse(sendingTime, FIX_TIME);
        long sentEpochMs = sent.toInstant(ZoneOffset.UTC).toEpochMilli();
        return Instant.now().toEpochMilli() - sentEpochMs <= maxAgeMs;
    }

    private void rejectAppMessage(SessionID sessionId, Message message, String text) throws FieldNotFound {
        String refMsgType = message.getHeader().getString(MsgType.FIELD);
        BusinessMessageReject reject = new BusinessMessageReject();
        reject.set(new quickfix.field.RefMsgType(refMsgType));
        reject.set(new quickfix.field.BusinessRejectReason(0));
        reject.set(new quickfix.field.Text(text));
        try {
            Session.sendToTarget(reject, sessionId);
        } catch (Exception e) {
            log.warn("FIX-in reject failed {}", text, e);
        }
    }
}
