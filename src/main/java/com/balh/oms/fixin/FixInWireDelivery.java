package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInQuickFixRawMessageRepository;
import com.balh.oms.fixin.persistence.FixInMessageAuditRow;
import com.balh.oms.fixin.persistence.FixInMessageAuditRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.field.ClOrdID;
import quickfix.field.MsgSeqNum;
import quickfix.field.MsgType;
import quickfix.field.OrigClOrdID;
import quickfix.fix44.OrderCancelReject;

import java.util.Optional;
import java.util.UUID;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInWireDelivery {

    private static final Logger log = LoggerFactory.getLogger(FixInWireDelivery.class);

    private final FixInSessionRegistry sessionRegistry;
    private final FixInMessageAuditRepository messageAuditRepository;
    private final OmsConfig omsConfig;
    private final FixInOpsMetrics metrics;

    public FixInWireDelivery(
            FixInSessionRegistry sessionRegistry,
            FixInMessageAuditRepository messageAuditRepository,
            OmsConfig omsConfig,
            FixInOpsMetrics metrics) {
        this.sessionRegistry = sessionRegistry;
        this.messageAuditRepository = messageAuditRepository;
        this.omsConfig = omsConfig;
        this.metrics = metrics;
    }

    public boolean sendToSession(UUID fixSessionId, Message message, String sessionRole, UUID omsOrderIdOrNull) {
        Optional<SessionID> wireId = sessionRegistry.findWireSessionId(fixSessionId);
        if (wireId.isEmpty()) {
            log.debug("FIX-in outbound skipped — session {} not logged on", fixSessionId);
            return false;
        }
        try {
            Session.sendToTarget(message, wireId.get());
            auditOutbound(message, sessionRole, fixSessionId, omsOrderIdOrNull);
            return true;
        } catch (SessionNotFound e) {
            log.warn("FIX-in outbound failed — session gone {}", wireId.get(), e);
            return false;
        }
    }

    public void auditInbound(Message message, FixInSessionRow session, UUID omsOrderIdOrNull) {
        audit(message, "INBOUND", sessionRole(session), session.id(), omsOrderIdOrNull);
    }

    private void auditOutbound(Message message, String sessionRole, UUID fixSessionId, UUID omsOrderIdOrNull) {
        audit(message, "OUTBOUND", sessionRole, fixSessionId, omsOrderIdOrNull);
    }

    private void audit(Message message, String direction, String sessionRole, UUID fixSessionId, UUID omsOrderId) {
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            String clOrdId = message.isSetField(ClOrdID.FIELD) ? message.getString(ClOrdID.FIELD) : null;
            String origClOrdId = message.isSetField(OrigClOrdID.FIELD) ? message.getString(OrigClOrdID.FIELD) : null;
            Integer msgSeq = message.getHeader().isSetField(MsgSeqNum.FIELD)
                    ? message.getHeader().getInt(MsgSeqNum.FIELD)
                    : null;
            String rawRef = null;
            if (omsConfig.getFixIn().isJdbcSessionStore() && msgSeq != null) {
                var wire = sessionRegistry.findWireSessionId(fixSessionId);
                if (wire.isPresent()) {
                    rawRef = FixInQuickFixRawMessageRepository.rawStoreRef(
                            wire.get().getSenderCompID(), wire.get().getTargetCompID(), msgSeq);
                }
            }
            messageAuditRepository.insert(new FixInMessageAuditRow(
                    UUID.randomUUID(),
                    direction,
                    sessionRole,
                    fixSessionId,
                    msgType,
                    msgSeq,
                    clOrdId,
                    origClOrdId,
                    omsOrderId,
                    null,
                    rawRef,
                    message.getClass().getSimpleName()));
        } catch (Exception e) {
            log.debug("FIX message audit skipped", e);
            metrics.auditWriteFailed();
        }
    }

    static String sessionRole(FixInSessionRow session) {
        return "DROP_COPY".equalsIgnoreCase(session.sessionMode())
                ? sessionRoleDropCopy()
                : sessionRoleOrderEntry();
    }

    static String sessionRoleOrderEntry() {
        return "FIX_IN_ORDER_ENTRY";
    }

    static String sessionRoleDropCopy() {
        return "FIX_IN_DROP_COPY";
    }
}
