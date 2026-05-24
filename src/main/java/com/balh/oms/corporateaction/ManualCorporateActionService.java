package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.util.Locale;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Ops manual CA templates with four-eyes approve (gap plan §5.9 Phase 1 manual). */
@Service
public class ManualCorporateActionService {

    public record CreateResult(long id, String status) {}

    public record ApproveResult(long id, long corporateActionEventId, String status) {}

    private final ManualCorporateActionEventRepository manualEvents;
    private final CorporateActionEventRepository corporateActionEvents;
    private final ObjectMapper objectMapper;

    public ManualCorporateActionService(
            ManualCorporateActionEventRepository manualEvents,
            CorporateActionEventRepository corporateActionEvents,
            ObjectMapper objectMapper) {
        this.manualEvents = manualEvents;
        this.corporateActionEvents = corporateActionEvents;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public CreateResult create(String templateType, JsonNode payload, String createdBy) {
        ManualCorporateActionEventRepository.validateTemplatePayload(templateType, payload, objectMapper);
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid payload_json");
        }
        long id = manualEvents.insert(templateType.trim().toUpperCase(Locale.ROOT), payloadJson, createdBy);
        return new CreateResult(id, "pending_approval");
    }

    @Transactional
    public Optional<ApproveResult> approve(long id, String approver, String createdBy) {
        if (approver == null || approver.isBlank()) {
            throw new IllegalArgumentException("approver required");
        }
        if (approver.equals(createdBy)) {
            throw new IllegalStateException("four-eyes: approver must differ from creator");
        }
        Optional<ManualCorporateActionEventRepository.Row> rowOpt = manualEvents.findById(id);
        if (rowOpt.isEmpty()) {
            return Optional.empty();
        }
        ManualCorporateActionEventRepository.Row row = rowOpt.get();
        if (!"pending_approval".equals(row.status())) {
            throw new IllegalStateException("manual event not pending approval");
        }
        if (approver.equals(row.createdBy())) {
            throw new IllegalStateException("four-eyes: approver must differ from creator");
        }
        JsonNode payload;
        try {
            payload = objectMapper.readTree(row.payloadJson());
        } catch (Exception e) {
            throw new IllegalStateException("invalid stored payload");
        }
        String symbol = payload.get("instrumentSymbol").asText().trim().toUpperCase(Locale.ROOT);
        LocalDate effectiveDate = LocalDate.parse(payload.get("effectiveDate").asText());
        String actionType = mapTemplateToActionType(row.templateType());
        long eventId =
                corporateActionEvents.insert(symbol, actionType, effectiveDate, row.payloadJson());
        if (!manualEvents.approve(id, approver, eventId)) {
            throw new IllegalStateException("approve CAS failed");
        }
        return Optional.of(new ApproveResult(id, eventId, "applied"));
    }

    private static String mapTemplateToActionType(String templateType) {
        return switch (templateType.trim().toUpperCase(Locale.ROOT)) {
            case "MERGER" -> "MERGER";
            case "SPIN_OFF" -> "SPIN_OFF";
            case "BANKRUPTCY_DELISTING" -> "BANKRUPTCY_DELISTING";
            default -> templateType;
        };
    }
}
