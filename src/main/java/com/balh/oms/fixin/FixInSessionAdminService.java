package com.balh.oms.fixin;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInMessageAuditRepository;
import com.balh.oms.fixin.persistence.FixInMessageAuditRow;
import com.balh.oms.fixin.persistence.FixInQuickFixRawMessageRepository;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInSessionAdminService {

    private final FixInSessionRepository sessionRepository;
    private final FixInSessionAdminActionRepository adminActionRepository;
    private final FixInSessionRegistry sessionRegistry;
    private final FixInSessionRuntimeService runtimeService;
    private final FixInMessageAuditRepository messageAuditRepository;
    private final FixInQuickFixRawMessageRepository rawMessageRepository;
    private final FixInOpsMetrics metrics;
    private final ObjectMapper objectMapper;

    public FixInSessionAdminService(
            FixInSessionRepository sessionRepository,
            FixInSessionAdminActionRepository adminActionRepository,
            FixInSessionRegistry sessionRegistry,
            FixInSessionRuntimeService runtimeService,
            FixInMessageAuditRepository messageAuditRepository,
            FixInQuickFixRawMessageRepository rawMessageRepository,
            FixInOpsMetrics metrics,
            ObjectMapper objectMapper) {
        this.sessionRepository = sessionRepository;
        this.adminActionRepository = adminActionRepository;
        this.sessionRegistry = sessionRegistry;
        this.runtimeService = runtimeService;
        this.messageAuditRepository = messageAuditRepository;
        this.rawMessageRepository = rawMessageRepository;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    public List<EnrichedSession> listEnrichedSessions() {
        return sessionRepository.findAll().stream().map(this::enrich).toList();
    }

    public Optional<EnrichedSession> getEnriched(UUID sessionId) {
        return sessionRepository.findById(sessionId).map(this::enrich);
    }

    public void forceLogout(UUID sessionId, String requestedBy, String reason) {
        FixInSessionRow row = requireSession(sessionId);
        SessionID wireId = FixInSessionRuntimeService.wireSessionId(row);
        Session session = Session.lookupSession(wireId);
        if (session != null && session.isLoggedOn()) {
            session.logout("operator_requested");
        }
        sessionRegistry.unregister(wireId);
        recordAdminAction(row, "FORCE_LOGOUT", requestedBy, null, reason, null, Map.of("wireSession", wireId.toString()));
        metrics.adminActionSucceeded();
    }

    public void rotateCredential(
            UUID sessionId, String requestedBy, String approvedBy, String reason, String newPasswordHash) {
        if (newPasswordHash == null || newPasswordHash.isBlank()) {
            throw new IllegalArgumentException("password_hash_required");
        }
        FixInSessionRow row = requireSession(sessionId);
        if (!sessionRepository.updatePasswordHash(sessionId, newPasswordHash.trim())) {
            throw new IllegalStateException("credential_update_failed");
        }
        recordAdminAction(
                row,
                "CREDENTIAL_ROTATION",
                requestedBy,
                approvedBy,
                reason,
                null,
                Map.of("staged", true));
        metrics.adminActionSucceeded();
    }

    public List<FixInSessionAdminActionRow> listAdminActions(UUID sessionIdOrNull, int limit) {
        return adminActionRepository.list(sessionIdOrNull, limit);
    }

    public List<FixInMessageAuditRow> searchMessageAudit(
            UUID sessionIdOrNull, String clOrdIdOrNull, UUID omsOrderIdOrNull, int limit) {
        return messageAuditRepository.search(sessionIdOrNull, clOrdIdOrNull, omsOrderIdOrNull, limit);
    }

    public Optional<MessageAuditDetail> messageAuditDetail(UUID auditId) {
        return messageAuditRepository
                .findById(auditId)
                .map(row -> new MessageAuditDetail(
                        row,
                        row.rawStoreRefOrNull() == null
                                ? Optional.empty()
                                : rawMessageRepository.findRedactedByRawStoreRef(row.rawStoreRefOrNull())));
    }

    private EnrichedSession enrich(FixInSessionRow row) {
        return new EnrichedSession(row, runtimeService.runtime(row));
    }

    private FixInSessionRow requireSession(UUID sessionId) {
        return sessionRepository.findById(sessionId).orElseThrow(() -> new IllegalArgumentException("unknown_session"));
    }

    private void recordAdminAction(
            FixInSessionRow row,
            String actionType,
            String requestedBy,
            String approvedBy,
            String reason,
            String counterpartyReference,
            Map<String, ?> payload) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            payloadJson = "{}";
        }
        adminActionRepository.insert(new FixInSessionAdminActionRow(
                UUID.randomUUID(),
                FixInWireDelivery.sessionRole(row),
                row.id(),
                null,
                actionType,
                requestedBy,
                approvedBy,
                reason,
                counterpartyReference,
                payloadJson));
    }

    public record EnrichedSession(FixInSessionRow config, FixInSessionRuntimeService.RuntimeView runtime) {}

    public record MessageAuditDetail(FixInMessageAuditRow audit, Optional<String> redactedFixTextOrEmpty) {}
}
