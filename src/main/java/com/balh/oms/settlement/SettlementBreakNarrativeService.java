package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementBreakNarrativeResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Service;

/** Deterministic break explanations for MCP investigators (gap plan §10.2). */
@Service
public class SettlementBreakNarrativeService {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ReconciliationBreakRepository breaks;
    private final ObjectMapper objectMapper;

    public SettlementBreakNarrativeService(
            ReconciliationBreakRepository breaks, ObjectMapper objectMapper) {
        this.breaks = breaks;
        this.objectMapper = objectMapper;
    }

    public Optional<SettlementBreakNarrativeResponse> narrate(long breakId) {
        return breaks.findById(breakId).map(this::build);
    }

    private SettlementBreakNarrativeResponse build(ReconciliationBreakRepository.BreakRow row) {
        Map<String, Object> structured = parseDiffJson(row.diffJson());
        NarrativeParts parts = narrativeForType(row.breakType(), structured, row);
        List<String> relatedExecIds = new ArrayList<>();
        if (row.executionId() != null) {
            relatedExecIds.add(String.valueOf(row.executionId()));
        }
        Object execRef = structured.get("executionId");
        if (execRef != null && !relatedExecIds.contains(String.valueOf(execRef))) {
            relatedExecIds.add(String.valueOf(execRef));
        }
        return new SettlementBreakNarrativeResponse(
                row.id(), row, parts.summary(), parts.recommendedAction(), structured, relatedExecIds);
    }

    private Map<String, Object> parseDiffJson(String diffJson) {
        if (diffJson == null || diffJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(diffJson, MAP_TYPE);
        } catch (Exception e) {
            return new LinkedHashMap<>(Map.of("raw", diffJson, "parseError", e.getMessage()));
        }
    }

    private NarrativeParts narrativeForType(
            String breakType, Map<String, Object> diff, ReconciliationBreakRepository.BreakRow row) {
        if (breakType == null) {
            return new NarrativeParts(
                    "Unknown break type on reconciliation break " + row.id() + ".",
                    "Inspect diff_json and linked execution in Beard Admin /settlement-recon.");
        }
        return switch (breakType) {
            case ReconciliationBreakRepository.BREAK_CASH_MISMATCH ->
                    cashMismatchNarrative(diff, row);
            case ReconciliationBreakRepository.BREAK_TRADE_MISMATCH ->
                    new NarrativeParts(
                            "Broker trade confirm economics disagree with the OMS execution.",
                            "Compare broker row vs OMS quantity/price/gross in the evidence pack; "
                                    + "resolve via broker correction or waive if bench noise.");
            case ReconciliationBreakRepository.BREAK_SETTLEMENT_DATE_MISMATCH ->
                    new NarrativeParts(
                            "Broker settlement date differs from OMS expected settlement date.",
                            "Check instrument_settlement_profile and settlement calendar; "
                                    + "broker date is authoritative for actual settlement.");
            case ReconciliationBreakRepository.BREAK_POSITION_MISMATCH ->
                    new NarrativeParts(
                            "Broker position snapshot quantity differs from OMS positions.",
                            "Run position reconciliation report; verify custody account mapping and pending buys/sells.");
            case ReconciliationBreakRepository.BREAK_UNRESOLVED_CONFIRM ->
                    new NarrativeParts(
                            "Broker confirm row could not be matched to an OMS execution.",
                            "Verify venueExecRef/accountId on the confirm row matches a TRADE execution.");
            case ReconciliationBreakRepository.BREAK_CORPORATE_ACTION_MISMATCH ->
                    new NarrativeParts(
                            "Broker corporate-action row drift vs OMS corporate_action_event.",
                            "Compare broker CA batch to OMS event; apply manual CA template if needed.");
            case ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_UNMATCHED ->
                    new NarrativeParts(
                            "Broker settlement-fail row did not resolve to an OMS execution.",
                            "Check fail file execution reference against venue_exec_ref.");
            case ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_QUANTITY_MISMATCH ->
                    new NarrativeParts(
                            "Broker failed quantity exceeds OMS trade quantity.",
                            "Review partial-fail lots and broker fail apply workflow.");
            default ->
                    new NarrativeParts(
                            "Open reconciliation break (" + breakType + ").",
                            "Inspect diff_json at /settlement-recon and correlate linked execution if present.");
        };
    }

    private NarrativeParts cashMismatchNarrative(
            Map<String, Object> diff, ReconciliationBreakRepository.BreakRow row) {
        Object reason = diff.get("reason");
        if ("currency mismatch".equals(String.valueOf(diff.get("reason")))
                || (reason != null && reason.toString().toLowerCase().contains("currency"))) {
            return new NarrativeParts(
                    "Cash reconciliation reported a currency mismatch between broker movement and OMS expectation.",
                    "Check instrument_settlement_profile.settlement_currency for the symbol; "
                            + "historical bug used hardcoded SEK — re-run recon after profile sync.");
        }
        if ("alert_fault_injection".equals(String.valueOf(reason))) {
            return new NarrativeParts(
                    "Synthetic break inserted for alert fault-injection testing.",
                    "DELETE this row after verifying Prometheus/Alertmanager; not a production break.");
        }
        return new NarrativeParts(
                "Broker cash statement movement disagrees with OMS execution cash expectation.",
                "Open cash reconciliation report for the business date; compare signed gross amounts.");
    }

    private record NarrativeParts(String summary, String recommendedAction) {}
}
