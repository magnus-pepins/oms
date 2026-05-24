package com.balh.oms.ingress;

import com.balh.oms.persistence.ManualSettlementActionRow;
import com.balh.oms.settlement.ReconciliationBreakRepository;
import java.util.List;

/**
 * Ops/regulatory evidence bundle for one execution (gap plan §5.17).
 *
 * <p>Aggregates OMS execution, settlement timeline, broker confirms, reconciliation breaks,
 * and manual settlement actions — no log scraping required.
 */
public record SettlementEvidencePackResponse(
        long executionId,
        SettlementExecutionDetailResponse execution,
        SettlementTimelineResponse timeline,
        List<BrokerTradeConfirmEvidence> brokerTradeConfirms,
        V1BrokerConfirmEvidence v1BrokerConfirm,
        List<ReconciliationBreakRepository.BreakRow> reconciliationBreaks,
        List<ManualSettlementActionEvidence> manualSettlementActions) {

    public record BrokerTradeConfirmEvidence(
            long confirmId,
            long batchId,
            String brokerId,
            String brokerTradeId,
            String matchStatus,
            String matchDiffJson,
            String rawRowJson) {}

    public record V1BrokerConfirmEvidence(long confirmId, java.time.Instant createdAt, java.time.Instant appliedAt) {}

    public record ManualSettlementActionEvidence(
            long id,
            String actionType,
            String requestedBy,
            String approvedBy,
            String payloadJson,
            java.time.Instant createdAt) {

        public static ManualSettlementActionEvidence from(ManualSettlementActionRow row) {
            return new ManualSettlementActionEvidence(
                    row.id(),
                    row.actionType(),
                    row.requestedBy(),
                    row.approvedBy(),
                    row.payloadJson(),
                    row.createdAt());
        }
    }
}
