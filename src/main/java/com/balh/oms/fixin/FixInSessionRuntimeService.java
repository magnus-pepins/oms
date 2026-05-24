package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInQuickFixSessionStateRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.util.Optional;

@Service
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInSessionRuntimeService {

    private final FixInSessionRegistry sessionRegistry;
    private final FixInQuickFixSessionStateRepository quickFixSessionStateRepository;
    private final OmsConfig omsConfig;

    public FixInSessionRuntimeService(
            FixInSessionRegistry sessionRegistry,
            FixInQuickFixSessionStateRepository quickFixSessionStateRepository,
            OmsConfig omsConfig) {
        this.sessionRegistry = sessionRegistry;
        this.quickFixSessionStateRepository = quickFixSessionStateRepository;
        this.omsConfig = omsConfig;
    }

    public RuntimeView runtime(FixInSessionRow row) {
        SessionID wireId = wireSessionId(row);
        Optional<SessionID> activeWire = sessionRegistry.findWireSessionId(row.id());
        boolean loggedOn = activeWire.isPresent();
        int nextSender = 0;
        int nextTarget = 0;
        Session session = Session.lookupSession(wireId);
        if (session != null) {
            nextSender = session.getExpectedSenderNum();
            nextTarget = session.getExpectedTargetNum();
            loggedOn = session.isLoggedOn();
        }
        Optional<FixInQuickFixSessionStateRepository.SeqState> jdbcSeq =
                quickFixSessionStateRepository.findSeqState(
                        row.targetCompId(), row.senderCompId(), row.sessionQualifierOrNull());
        return new RuntimeView(
                loggedOn,
                activeWire.map(SessionID::toString).orElse(null),
                nextSender,
                nextTarget,
                jdbcSeq.map(FixInQuickFixSessionStateRepository.SeqState::incomingSeqNum).orElse(null),
                jdbcSeq.map(FixInQuickFixSessionStateRepository.SeqState::outgoingSeqNum).orElse(null),
                omsConfig.getFixIn().getSessionStoreType());
    }

    static SessionID wireSessionId(FixInSessionRow row) {
        return new SessionID(
                new BeginString("FIX.4.4"),
                new SenderCompID(row.targetCompId()),
                new TargetCompID(row.senderCompId()));
    }

    public record RuntimeView(
            boolean loggedOn,
            String activeWireSessionIdOrNull,
            int nextSenderSeq,
            int nextTargetSeq,
            Integer jdbcIncomingSeqOrNull,
            Integer jdbcOutgoingSeqOrNull,
            String storeBackend) {}
}
