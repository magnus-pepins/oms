package com.balh.oms.fixin;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInSessionAdminActionRepository;
import com.balh.oms.fixin.persistence.FixInSessionAdminActionRow;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Audited sequence reset for FIX-in sessions. Requires session disabled or logged out before
 * mutating QuickFIX sequence numbers (Ops Console workflow).
 */
@Service
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInSequenceAdminService {

    private final FixInSessionRepository sessionRepository;
    private final FixInSessionAdminActionRepository adminActionRepository;
    private final FixInSessionRegistry sessionRegistry;
    private final ObjectMapper objectMapper;

    public FixInSequenceAdminService(
            FixInSessionRepository sessionRepository,
            FixInSessionAdminActionRepository adminActionRepository,
            FixInSessionRegistry sessionRegistry,
            ObjectMapper objectMapper) {
        this.sessionRepository = sessionRepository;
        this.adminActionRepository = adminActionRepository;
        this.sessionRegistry = sessionRegistry;
        this.objectMapper = objectMapper;
    }

    public void applySequenceReset(
            UUID sessionId,
            String requestedBy,
            String approvedBy,
            String reason,
            String counterpartyReference,
            int nextSenderSeq,
            int nextTargetSeq) {
        FixInSessionRow row = sessionRepository
                .findById(sessionId)
                .orElseThrow(() -> new IllegalArgumentException("unknown_session"));
        if (sessionRegistry.findWireSessionId(sessionId).isPresent()) {
            throw new IllegalStateException("session_must_be_logged_out_before_sequence_reset");
        }
        SessionID wireId = new SessionID(
                new BeginString("FIX.4.4"),
                new SenderCompID(row.targetCompId()),
                new TargetCompID(row.senderCompId()));
        Session session = Session.lookupSession(wireId);
        if (session != null) {
            try {
                session.setNextSenderMsgSeqNum(nextSenderSeq);
                session.setNextTargetMsgSeqNum(nextTargetSeq);
            } catch (IOException e) {
                throw new IllegalStateException("sequence_reset_failed", e);
            }
        }
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(Map.of(
                    "nextSenderSeq", nextSenderSeq,
                    "nextTargetSeq", nextTargetSeq,
                    "wireSession", wireId.toString()));
        } catch (JsonProcessingException e) {
            payloadJson = "{\"error\":\"payload_encode_failed\"}";
        }
        adminActionRepository.insert(new FixInSessionAdminActionRow(
                UUID.randomUUID(),
                FixInWireDelivery.sessionRole(row),
                sessionId,
                null,
                "SEQUENCE_RESET",
                requestedBy,
                approvedBy,
                reason,
                counterpartyReference,
                payloadJson,
                java.time.Instant.now()));
    }
}
