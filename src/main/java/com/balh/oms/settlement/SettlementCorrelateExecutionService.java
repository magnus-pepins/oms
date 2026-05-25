package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementCorrelateExecutionResponse;
import com.balh.oms.ingress.SettlementEvidencePackResponse;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Service;

/** Composes evidence-pack data with settlement profile, cash expectation, and batch SHAs. */
@Service
public class SettlementCorrelateExecutionService {

    private static final String DEFAULT_BROKER_ID = "DEFAULT";

    private final SettlementEvidencePackService evidencePackService;
    private final InstrumentSettlementProfileRepository instrumentProfiles;
    private final CashReconciliationLookupRepository cashLookups;
    private final BrokerConfirmBatchRepository brokerConfirmBatches;
    private final SettlementCustomerNotificationOutboxRepository customerNotifications;

    public SettlementCorrelateExecutionService(
            SettlementEvidencePackService evidencePackService,
            InstrumentSettlementProfileRepository instrumentProfiles,
            CashReconciliationLookupRepository cashLookups,
            BrokerConfirmBatchRepository brokerConfirmBatches,
            SettlementCustomerNotificationOutboxRepository customerNotifications) {
        this.evidencePackService = evidencePackService;
        this.instrumentProfiles = instrumentProfiles;
        this.cashLookups = cashLookups;
        this.brokerConfirmBatches = brokerConfirmBatches;
        this.customerNotifications = customerNotifications;
    }

    public Optional<SettlementCorrelateExecutionResponse> correlate(long executionId) {
        Optional<SettlementEvidencePackResponse> packOpt = evidencePackService.load(executionId);
        if (packOpt.isEmpty()) {
            return Optional.empty();
        }
        SettlementEvidencePackResponse pack = packOpt.get();
        var execution = pack.execution();

        LocalDate profileAsOf =
                execution.tradeDate() != null
                        ? execution.tradeDate()
                        : execution.venueTs() == null
                                ? LocalDate.now(java.time.ZoneOffset.UTC)
                                : execution.venueTs().atZone(java.time.ZoneOffset.UTC).toLocalDate();

        InstrumentSettlementProfile profile =
                instrumentProfiles
                        .findActiveBySymbol(execution.instrumentSymbol(), profileAsOf)
                        .orElse(null);

        String brokerId = DEFAULT_BROKER_ID;
        if (!pack.brokerTradeConfirms().isEmpty()) {
            String fromConfirm = pack.brokerTradeConfirms().getFirst().brokerId();
            if (fromConfirm != null && !fromConfirm.isBlank()) {
                brokerId = fromConfirm;
            }
        }

        CashReconciliationLookupRepository.ExecutionCashExpectation cashExpectation = null;
        if (execution.venueExecRef() != null && !execution.venueExecRef().isBlank()) {
            cashExpectation =
                    cashLookups.findByBrokerAndVenueRef(brokerId, execution.venueExecRef()).orElse(null);
        }

        Set<Long> batchIds = new LinkedHashSet<>();
        for (SettlementEvidencePackResponse.BrokerTradeConfirmEvidence c : pack.brokerTradeConfirms()) {
            batchIds.add(c.batchId());
        }

        List<SettlementCorrelateExecutionResponse.BrokerConfirmBatchSummary> batches = new ArrayList<>();
        for (long batchId : batchIds) {
            brokerConfirmBatches
                    .findById(batchId)
                    .ifPresent(
                            b ->
                                    batches.add(
                                            new SettlementCorrelateExecutionResponse.BrokerConfirmBatchSummary(
                                                    b.id(),
                                                    b.brokerId(),
                                                    b.brokerFileId(),
                                                    b.fileSha256Hex(),
                                                    b.status(),
                                                    b.appliedAt())));
        }

        List<SettlementCorrelateExecutionResponse.CustomerNotificationSummary> notifications =
                customerNotifications.listByExecutionId(executionId).stream()
                        .map(
                                n ->
                                        new SettlementCorrelateExecutionResponse.CustomerNotificationSummary(
                                                n.id(),
                                                n.notificationType(),
                                                n.attempts(),
                                                n.publishedAt() != null))
                        .toList();

        return Optional.of(
                new SettlementCorrelateExecutionResponse(
                        executionId,
                        pack,
                        profile,
                        cashExpectation,
                        batches,
                        notifications));
    }
}
