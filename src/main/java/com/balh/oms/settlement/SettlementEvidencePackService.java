package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementEvidencePackResponse;
import com.balh.oms.ingress.SettlementExecutionDetailResponse;
import com.balh.oms.ingress.SettlementTimelineResponse;
import com.balh.oms.persistence.ManualSettlementActionsRepository;
import com.balh.oms.persistence.SettlementExecutionDetailRow;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

/** Builds {@link SettlementEvidencePackResponse} for gap plan §5.17. */
@Service
public class SettlementEvidencePackService {

    private static final int MANUAL_ACTIONS_EVIDENCE_LIMIT = 50;

    private final SettlementExecutionsRepository executions;
    private final SettlementTimelineService timelineService;
    private final BrokerTradeConfirmRepository brokerTradeConfirms;
    private final BrokerSettlementConfirmRepository v1BrokerConfirms;
    private final ReconciliationBreakRepository reconciliationBreaks;
    private final ManualSettlementActionsRepository manualActions;

    public SettlementEvidencePackService(
            SettlementExecutionsRepository executions,
            SettlementTimelineService timelineService,
            BrokerTradeConfirmRepository brokerTradeConfirms,
            BrokerSettlementConfirmRepository v1BrokerConfirms,
            ReconciliationBreakRepository reconciliationBreaks,
            ManualSettlementActionsRepository manualActions) {
        this.executions = executions;
        this.timelineService = timelineService;
        this.brokerTradeConfirms = brokerTradeConfirms;
        this.v1BrokerConfirms = v1BrokerConfirms;
        this.reconciliationBreaks = reconciliationBreaks;
        this.manualActions = manualActions;
    }

    public Optional<SettlementEvidencePackResponse> load(long executionId) {
        Optional<SettlementExecutionDetailRow> execOpt = executions.findById(executionId);
        if (execOpt.isEmpty()) {
            return Optional.empty();
        }
        SettlementExecutionDetailRow exec = execOpt.get();
        SettlementTimelineResponse timeline =
                timelineService.loadTimeline(executionId).orElseThrow();

        SettlementExecutionDetailResponse executionDetail =
                new SettlementExecutionDetailResponse(
                        exec.id(),
                        exec.orderId(),
                        exec.accountId(),
                        exec.venueId(),
                        exec.venueTs(),
                        exec.venueExecRef(),
                        exec.lastQuantity(),
                        exec.lastPrice(),
                        exec.leavesQuantity(),
                        exec.cumQuantityAfter(),
                        exec.execType(),
                        exec.settlementStatus(),
                        exec.createdAt(),
                        exec.orderStatus(),
                        exec.side(),
                        exec.instrumentSymbol(),
                        exec.tradeDate(),
                        exec.expectedSettlementDate(),
                        exec.rawEnvelopeJson());

        List<SettlementEvidencePackResponse.BrokerTradeConfirmEvidence> v2Confirms =
                brokerTradeConfirms.findEvidenceByResolvedExecutionId(executionId).stream()
                        .map(r -> new SettlementEvidencePackResponse.BrokerTradeConfirmEvidence(
                                r.confirmId(),
                                r.batchId(),
                                r.brokerId(),
                                r.brokerTradeId(),
                                r.matchStatus(),
                                r.matchDiffJson(),
                                r.rawRowJson()))
                        .toList();

        SettlementEvidencePackResponse.V1BrokerConfirmEvidence v1 =
                v1BrokerConfirms
                        .findByExecution(executionId)
                        .map(c -> new SettlementEvidencePackResponse.V1BrokerConfirmEvidence(
                                c.id(), c.createdAt(), c.appliedAt()))
                        .orElse(null);

        List<ReconciliationBreakRepository.BreakRow> breaks =
                reconciliationBreaks.listByExecutionId(executionId);

        List<SettlementEvidencePackResponse.ManualSettlementActionEvidence> manual =
                manualActions.findByFilters(executionId, null, null, MANUAL_ACTIONS_EVIDENCE_LIMIT, 0).stream()
                        .map(SettlementEvidencePackResponse.ManualSettlementActionEvidence::from)
                        .toList();

        return Optional.of(new SettlementEvidencePackResponse(
                executionId,
                executionDetail,
                timeline,
                v2Confirms,
                v1,
                breaks,
                manual));
    }
}
