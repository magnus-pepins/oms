package com.balh.oms.ingress;

import com.balh.oms.settlement.ReconciliationBreakRepository;
import java.util.List;
import java.util.Map;

/** Structured break explanation for MCP / ops (gap plan §10.2 {@code getBreakNarrative}). */
public record SettlementBreakNarrativeResponse(
        long breakId,
        ReconciliationBreakRepository.BreakRow breakRow,
        String summary,
        String recommendedAction,
        Map<String, Object> structuredDiff,
        List<String> relatedExecutionIds) {}
