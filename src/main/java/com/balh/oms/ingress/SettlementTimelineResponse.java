package com.balh.oms.ingress;

import java.time.Instant;
import java.time.LocalDate;
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
 * <p>{@code tradeDate} and {@code expectedSettlementDate} pull through the columns
 * populated by {@link com.balh.oms.settlement.SettlementDateCalculator} at TRADE insert
 * time (gap plan §5.3 Slice 1 / 2b). Both are {@code null} on CANCEL / REJECT / REPLACE
 * rows and on legacy executions written before V58 / V62; consumers must therefore
 * render "—" rather than guess. The customer-frontend BFF surfaces
 * {@code expectedSettlementDate} as "Settles on …" in the order detail screen.
 *
 * <p>Phases that don't yield a recorded timestamp (e.g. {@code settling} is advanced in
 * the same DB transaction as {@code settled} on the auto-step path) are omitted rather
 * than guessed. The beard-admin Detail panel renders this as a vertical timeline.
 */
public record SettlementTimelineResponse(
        long executionId,
        String currentSettlementStatus,
        LocalDate tradeDate,
        LocalDate expectedSettlementDate,
        List<Phase> phases,
        List<LedgerLeg> ledgerLegs) {

    /**
     * One step in the lifecycle. {@code source} is a free-form provenance string
     * (e.g. {@code executions.created_at}, {@code broker_settlement_confirm.applied_at})
     * so operators investigating breaks can trace where the timestamp came from.
     */
    public record Phase(String phase, Instant occurredAt, String source) {}

    /**
     * One Ledger posting attempt for a settlement leg.
     *
     * <p>{@code postedAt == null} means the reconciler hasn't successfully posted yet
     * (still retrying or hard-failed). {@code attempts} / {@code lastErrorText} /
     * {@code lastAttemptAt} expose the per-row forensic state added by V42 so operators
     * can see how many times the row has been retried and what the last error was.
     * {@code skippedAt} / {@code skipReason} are set by the reconciler's V43 tombstone
     * path when the row is no longer retriable (operator/data gap, e.g. unfunded
     * balance or indicator-not-found — distinct from a transient Ledger 503).
     */
    public record LedgerLeg(
            long outboxId,
            String legKind,
            String toSettlementStatus,
            Instant enqueuedAt,
            Instant postedAt,
            int attempts,
            String lastErrorText,
            Instant lastAttemptAt,
            Instant skippedAt,
            String skipReason,
            String payloadJson) {}
}
