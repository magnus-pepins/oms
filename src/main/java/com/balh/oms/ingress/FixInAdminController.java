package com.balh.oms.ingress;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.FixInSequenceAdminService;
import com.balh.oms.fixin.FixInSessionAdminService;
import com.balh.oms.fixin.FixInSessionRuntimeService;
import com.balh.oms.fixin.persistence.FixInMessageAuditRow;
import com.balh.oms.fixin.persistence.FixInSessionAdminActionRow;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Internal admin API for FIX-in sessions (Ops Console proxy target). */
@RestController
@RequestMapping("/internal/v1/fix-in")
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInAdminController {

    private final FixInSessionRepository sessionRepository;
    private final FixInSequenceAdminService sequenceAdminService;
    private final FixInSessionAdminService sessionAdminService;

    public FixInAdminController(
            FixInSessionRepository sessionRepository,
            FixInSequenceAdminService sequenceAdminService,
            FixInSessionAdminService sessionAdminService) {
        this.sessionRepository = sessionRepository;
        this.sequenceAdminService = sequenceAdminService;
        this.sessionAdminService = sessionAdminService;
    }

    public record SessionView(
            UUID id,
            String environment,
            String sessionMode,
            String senderCompId,
            String targetCompId,
            boolean enabled) {
        static SessionView from(FixInSessionRow row) {
            return new SessionView(
                    row.id(),
                    row.environment(),
                    row.sessionMode(),
                    row.senderCompId(),
                    row.targetCompId(),
                    row.enabled());
        }
    }

    public record RuntimeView(
            boolean loggedOn,
            String activeWireSessionIdOrNull,
            int nextSenderSeq,
            int nextTargetSeq,
            Integer jdbcIncomingSeqOrNull,
            Integer jdbcOutgoingSeqOrNull,
            String storeBackend) {
        static RuntimeView from(FixInSessionRuntimeService.RuntimeView r) {
            return new RuntimeView(
                    r.loggedOn(),
                    r.activeWireSessionIdOrNull(),
                    r.nextSenderSeq(),
                    r.nextTargetSeq(),
                    r.jdbcIncomingSeqOrNull(),
                    r.jdbcOutgoingSeqOrNull(),
                    r.storeBackend());
        }
    }

    public record EnrichedSessionView(SessionView session, RuntimeView runtime) {
        static EnrichedSessionView from(FixInSessionAdminService.EnrichedSession e) {
            return new EnrichedSessionView(SessionView.from(e.config()), RuntimeView.from(e.runtime()));
        }
    }

    public record MessageAuditView(
            UUID id,
            String direction,
            String sessionRole,
            UUID fixSessionIdOrNull,
            String msgTypeOrNull,
            Integer msgSeqNumOrNull,
            String clOrdIdOrNull,
            String origClOrdIdOrNull,
            UUID omsOrderIdOrNull,
            String execIdOrNull,
            String rawStoreRefOrNull,
            String summaryOrNull,
            Instant createdAt) {
        static MessageAuditView from(FixInMessageAuditRow row) {
            return new MessageAuditView(
                    row.id(),
                    row.direction(),
                    row.sessionRole(),
                    row.fixSessionIdOrNull(),
                    row.msgTypeOrNull(),
                    row.msgSeqNumOrNull(),
                    row.clOrdIdOrNull(),
                    row.origClOrdIdOrNull(),
                    row.omsOrderIdOrNull(),
                    row.execIdOrNull(),
                    row.rawStoreRefOrNull(),
                    row.summaryOrNull(),
                    row.createdAt());
        }
    }

    public record MessageAuditDetailView(MessageAuditView audit, String redactedFixTextOrNull) {}

    public record AdminActionView(
            UUID id,
            String sessionRole,
            UUID fixSessionIdOrNull,
            String brokerRouteKeyOrNull,
            String actionType,
            String requestedBy,
            String approvedByOrNull,
            String reason,
            String counterpartyReferenceOrNull,
            String payloadJsonOrNull,
            Instant createdAt) {
        static AdminActionView from(FixInSessionAdminActionRow row) {
            return new AdminActionView(
                    row.id(),
                    row.sessionRole(),
                    row.fixSessionIdOrNull(),
                    row.brokerRouteKeyOrNull(),
                    row.actionType(),
                    row.requestedBy(),
                    row.approvedByOrNull(),
                    row.reason(),
                    row.counterpartyReferenceOrNull(),
                    row.payloadJsonOrNull(),
                    row.createdAt());
        }
    }

    public record SessionEnabledRequest(boolean enabled) {}

    public record SequenceResetRequest(
            @NotBlank String requestedBy,
            String approvedBy,
            @NotBlank String reason,
            String counterpartyReference,
            int nextSenderSeq,
            int nextTargetSeq) {}

    public record LogoutRequest(@NotBlank String requestedBy, @NotBlank String reason) {}

    public record CredentialRotationRequest(
            @NotBlank String requestedBy,
            String approvedBy,
            @NotBlank String reason,
            @NotBlank String newPasswordHash) {}

    @GetMapping("/sessions")
    public List<EnrichedSessionView> listSessions() {
        return sessionAdminService.listEnrichedSessions().stream().map(EnrichedSessionView::from).toList();
    }

    @GetMapping("/sessions/{sessionId}")
    public ResponseEntity<EnrichedSessionView> getSession(@PathVariable UUID sessionId) {
        return sessionAdminService
                .getEnriched(sessionId)
                .map(e -> ResponseEntity.ok(EnrichedSessionView.from(e)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PatchMapping("/sessions/{sessionId}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable UUID sessionId, @RequestBody SessionEnabledRequest body) {
        if (!sessionRepository.updateEnabled(sessionId, body.enabled())) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of("sessionId", sessionId, "enabled", body.enabled()));
    }

    @PostMapping("/sessions/{sessionId}/sequence-reset")
    public ResponseEntity<?> sequenceReset(
            @PathVariable UUID sessionId, @Valid @RequestBody SequenceResetRequest body) {
        sequenceAdminService.applySequenceReset(
                sessionId,
                body.requestedBy(),
                body.approvedBy(),
                body.reason(),
                body.counterpartyReference(),
                body.nextSenderSeq(),
                body.nextTargetSeq());
        return ResponseEntity.accepted()
                .body(Map.of("sessionId", sessionId, "status", "sequence_reset_applied"));
    }

    @PostMapping("/sessions/{sessionId}/logout")
    public ResponseEntity<?> forceLogout(@PathVariable UUID sessionId, @Valid @RequestBody LogoutRequest body) {
        sessionAdminService.forceLogout(sessionId, body.requestedBy(), body.reason());
        return ResponseEntity.accepted().body(Map.of("sessionId", sessionId, "status", "logout_requested"));
    }

    @PostMapping("/sessions/{sessionId}/credential-rotation")
    public ResponseEntity<?> credentialRotation(
            @PathVariable UUID sessionId, @Valid @RequestBody CredentialRotationRequest body) {
        sessionAdminService.rotateCredential(
                sessionId, body.requestedBy(), body.approvedBy(), body.reason(), body.newPasswordHash());
        return ResponseEntity.accepted().body(Map.of("sessionId", sessionId, "status", "credential_staged"));
    }

    @GetMapping("/admin-actions")
    public List<AdminActionView> listAdminActions(
            @RequestParam(required = false) UUID sessionId, @RequestParam(defaultValue = "50") int limit) {
        return sessionAdminService.listAdminActions(sessionId, limit).stream()
                .map(AdminActionView::from)
                .toList();
    }

    @GetMapping("/message-audit")
    public List<MessageAuditView> searchMessageAudit(
            @RequestParam(required = false) UUID sessionId,
            @RequestParam(required = false) String clOrdId,
            @RequestParam(required = false) UUID omsOrderId,
            @RequestParam(defaultValue = "50") int limit) {
        return sessionAdminService.searchMessageAudit(sessionId, clOrdId, omsOrderId, limit).stream()
                .map(MessageAuditView::from)
                .toList();
    }

    @GetMapping("/message-audit/{auditId}")
    public ResponseEntity<MessageAuditDetailView> messageAuditDetail(@PathVariable UUID auditId) {
        return sessionAdminService
                .messageAuditDetail(auditId)
                .map(detail -> ResponseEntity.ok(new MessageAuditDetailView(
                        MessageAuditView.from(detail.audit()),
                        detail.redactedFixTextOrEmpty().orElse(null))))
                .orElse(ResponseEntity.notFound().build());
    }
}
