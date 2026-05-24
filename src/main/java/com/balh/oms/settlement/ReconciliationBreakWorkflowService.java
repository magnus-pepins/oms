package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Ops workflow for {@code reconciliation_breaks}: assign (→ investigating), resolve, waive.
 * Does not mutate OMS positions or Ledger cash — those remain manual settlement actions.
 */
@Service
public class ReconciliationBreakWorkflowService {

    public enum Outcome {
        APPLIED,
        NOT_FOUND,
        CONFLICT,
        INVALID_REQUEST
    }

    public record Result(Outcome outcome, ReconciliationBreakRepository.BreakRow breakRow) {}

    private static final int EVENT_LIST_MAX = 100;

    private final ReconciliationBreakRepository breaks;
    private final ReconciliationBreakEventRepository events;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public ReconciliationBreakWorkflowService(
            ReconciliationBreakRepository breaks,
            ReconciliationBreakEventRepository events,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.breaks = breaks;
        this.events = events;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    @Transactional
    public Result assign(long breakId, String assignedTo, String actor, String notes) {
        if (assignedTo == null || assignedTo.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (actor == null || actor.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (!breaks.findById(breakId).isPresent()) {
            return new Result(Outcome.NOT_FOUND, null);
        }
        String trimmedNotes = truncateNotes(notes);
        int updated = breaks.assignInvestigating(breakId, assignedTo.trim(), trimmedNotes);
        if (updated == 0) {
            return new Result(Outcome.CONFLICT, breaks.findById(breakId).orElse(null));
        }
        events.insert(breakId, "assigned", actor.trim(), payload("assignedTo", assignedTo.trim(), "notes", trimmedNotes));
        return new Result(Outcome.APPLIED, breaks.findById(breakId).orElseThrow());
    }

    @Transactional
    public Result resolve(long breakId, String resolutionCode, String resolutionNote, String actor) {
        if (!ReconciliationBreakResolutionCodes.isValidResolveCode(resolutionCode)) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (resolutionNote == null || resolutionNote.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (actor == null || actor.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (!breaks.findById(breakId).isPresent()) {
            return new Result(Outcome.NOT_FOUND, null);
        }
        String note = truncateNotes(resolutionNote);
        int updated = breaks.markResolved(breakId, resolutionCode.trim(), note, actor.trim());
        if (updated == 0) {
            return new Result(Outcome.CONFLICT, breaks.findById(breakId).orElse(null));
        }
        events.insert(
                breakId,
                "resolved",
                actor.trim(),
                payload("resolutionCode", resolutionCode.trim(), "resolutionNote", note));
        return new Result(Outcome.APPLIED, breaks.findById(breakId).orElseThrow());
    }

    @Transactional
    public Result waive(long breakId, String resolutionNote, String actor) {
        if (resolutionNote == null || resolutionNote.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (actor == null || actor.isBlank()) {
            return new Result(Outcome.INVALID_REQUEST, null);
        }
        if (!breaks.findById(breakId).isPresent()) {
            return new Result(Outcome.NOT_FOUND, null);
        }
        String note = truncateNotes(resolutionNote);
        int updated =
                breaks.markWaived(breakId, ReconciliationBreakResolutionCodes.WAIVED_OPS, note, actor.trim());
        if (updated == 0) {
            return new Result(Outcome.CONFLICT, breaks.findById(breakId).orElse(null));
        }
        events.insert(breakId, "waived", actor.trim(), payload("resolutionNote", note));
        return new Result(Outcome.APPLIED, breaks.findById(breakId).orElseThrow());
    }

    public List<ReconciliationBreakEventRepository.EventRow> listEvents(long breakId) {
        if (breaks.findById(breakId).isEmpty()) {
            return List.of();
        }
        return events.listByBreakId(breakId, EVENT_LIST_MAX);
    }

    private String truncateNotes(String notes) {
        if (notes == null) {
            return null;
        }
        int max = config.getSettlement().getReconciliationBreakNoteMaxChars();
        String trimmed = notes.trim();
        return trimmed.length() <= max ? trimmed : trimmed.substring(0, max);
    }

    private String payload(String k1, String v1) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(k1, v1);
        return serialize(node);
    }

    private String payload(String k1, String v1, String k2, String v2) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(k1, v1);
        if (v2 != null) {
            node.put(k2, v2);
        }
        return serialize(node);
    }

    private String serialize(ObjectNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("break event payload serialise failed", e);
        }
    }
}
