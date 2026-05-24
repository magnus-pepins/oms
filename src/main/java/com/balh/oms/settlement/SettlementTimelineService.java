package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementTimelineResponse;
import com.balh.oms.persistence.SettlementExecutionDetailRow;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * Builds {@link SettlementTimelineResponse} for {@code GET /internal/v1/settlement/executions/{id}/timeline}.
 *
 * <p>Pulls from three repositories (executions, broker_settlement_confirm, ledger_settlement_outbox)
 * and emits a flat list of phases ordered by {@code occurredAt}. Designed to be safe to call on
 * any execution, including cancel-ack / cancel-reject rows that never had a broker confirm or
 * outbox row — for those, only the initial {@code executed} phase is returned.
 */
@Service
public class SettlementTimelineService {

    private final SettlementExecutionsRepository executions;
    private final BrokerSettlementConfirmRepository brokerConfirms;
    private final LedgerSettlementOutboxRepository outbox;

    public SettlementTimelineService(
            SettlementExecutionsRepository executions,
            BrokerSettlementConfirmRepository brokerConfirms,
            LedgerSettlementOutboxRepository outbox) {
        this.executions = executions;
        this.brokerConfirms = brokerConfirms;
        this.outbox = outbox;
    }

    /** @return empty when the execution doesn't exist (controller maps to 404). */
    public Optional<SettlementTimelineResponse> loadTimeline(long executionId) {
        Optional<SettlementExecutionDetailRow> execOpt = executions.findById(executionId);
        if (execOpt.isEmpty()) {
            return Optional.empty();
        }
        SettlementExecutionDetailRow exec = execOpt.get();

        List<SettlementTimelineResponse.Phase> phases = new ArrayList<>();
        phases.add(
                new SettlementTimelineResponse.Phase(
                        "executed", exec.createdAt(), "executions.created_at"));

        Optional<BrokerSettlementConfirmRepository.ConfirmTimelineRow> confirmOpt =
                brokerConfirms.findByExecution(executionId);
        confirmOpt.ifPresent(c -> {
            phases.add(
                    new SettlementTimelineResponse.Phase(
                            "matched", c.createdAt(), "broker_settlement_confirm.created_at"));
            if (c.appliedAt() != null) {
                phases.add(
                        new SettlementTimelineResponse.Phase(
                                "confirmed",
                                c.appliedAt(),
                                "broker_settlement_confirm.applied_at"));
            }
        });

        List<LedgerSettlementOutboxRepository.OutboxTimelineRow> legs = outbox.findByExecution(executionId);

        // 'settled' phase derives from the first cash leg's enqueue time — that's the
        // moment SettlementConfirmProcessor reached the settled CAS in Postgres.
        // (Fee leg shares the same DB tx, so its created_at == cash leg's.)
        legs.stream()
                .filter(l ->
                        LedgerSettlementOutboxRepository.LEG_CASH.equals(l.legKind())
                                || LedgerSettlementOutboxRepository.LEG_CASH_BASE.equals(l.legKind()))
                .findFirst()
                .ifPresent(first ->
                        phases.add(
                                new SettlementTimelineResponse.Phase(
                                        "settled",
                                        first.createdAt(),
                                        "ledger_settlement_outbox.created_at(" + first.legKind() + ")")));

        List<SettlementTimelineResponse.LedgerLeg> ledgerLegs = legs.stream()
                .map(l ->
                        new SettlementTimelineResponse.LedgerLeg(
                                l.id(),
                                l.legKind(),
                                l.toSettlementStatus(),
                                l.createdAt(),
                                l.postedAt(),
                                l.attempts(),
                                l.lastErrorText(),
                                l.lastAttemptAt(),
                                l.skippedAt(),
                                l.skipReason(),
                                l.payloadJson()))
                .toList();

        return Optional.of(
                new SettlementTimelineResponse(
                        executionId,
                        exec.settlementStatus(),
                        exec.tradeDate(),
                        exec.expectedSettlementDate(),
                        phases,
                        ledgerLegs));
    }
}
