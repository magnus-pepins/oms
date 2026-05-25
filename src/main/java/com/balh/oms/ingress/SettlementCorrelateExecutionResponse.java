package com.balh.oms.ingress;

import com.balh.oms.settlement.CashReconciliationLookupRepository;
import com.balh.oms.settlement.InstrumentSettlementProfile;
import java.util.List;

/** Cross-system correlation for one execution (gap plan §10.2 {@code correlateExecution}). */
public record SettlementCorrelateExecutionResponse(
        long executionId,
        SettlementEvidencePackResponse evidencePack,
        InstrumentSettlementProfile instrumentSettlementProfile,
        CashReconciliationLookupRepository.ExecutionCashExpectation omsCashExpectation,
        List<BrokerConfirmBatchSummary> brokerConfirmBatches,
        List<CustomerNotificationSummary> customerNotifications) {

    public record BrokerConfirmBatchSummary(
            long batchId,
            String brokerId,
            String brokerFileId,
            String fileSha256Hex,
            String status,
            java.time.Instant appliedAt) {}

    public record CustomerNotificationSummary(
            long id,
            String notificationType,
            int attempts,
            boolean published) {}
}
