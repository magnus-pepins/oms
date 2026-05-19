package com.balh.oms.ingress;

import java.time.Instant;
import java.util.List;

/**
 * Settlement lifecycle for a single execution. Built from three sources:
 *
 * <ul>
 *   <li>{@code executions.created_at} → the {@code executed} phase (always present).
 *   <li>{@code broker_settlement_confirm.created_at} / {@code applied_at} → {@code matched} +
 *       {@code confirmed} phases (only when a broker confirm row exists).
 *   <li>{@code ledger_settlement_outbox} rows → one entry per leg ({@code cash}, {@code fee},
 *       and Phase 2 {@code cash-base}/{@code cash-quote}) with both {@code created_at} (when
 *       OMS reached the {@code settled} CAS) and {@code posted_at} (when the reconciler
 *       successfully posted to Ledger, or {@code null} if pending / failed).
 * </ul>
 *
 * <p>Phases that don't yield a recorded timestamp (e.g. {@code settling} is advanced in
 * the same DB transaction as {@code settled} on the auto-step path) are omitted rather
 * than guessed. The beard-admin Detail panel renders this as a vertical timeline.
 */
public record SettlementTimelineResponse(
        long executionId, String currentSettlementStatus, List<Phase> phases, List<LedgerLeg> ledgerLegs) {

    /**
     * One step in the lifecycle. {@code source} is a free-form provenance string
     * (e.g. {@code executions.created_at}, {@code broker_settlement_confirm.applied_at})
     * so operators investigating breaks can trace where the timestamp came from.
     */
    public record Phase(String phase, Instant occurredAt, String source) {}

    /**
     * One Ledger posting attempt for a settlement leg. {@code postedAt == null} means
     * the reconciler hasn't successfully posted yet (still retrying or hard-failed).
     */
    public record LedgerLeg(
            long outboxId,
            String legKind,
            String toSettlementStatus,
            Instant enqueuedAt,
            Instant postedAt,
            String payloadJson) {}
}
