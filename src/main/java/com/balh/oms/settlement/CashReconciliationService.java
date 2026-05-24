package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Compares a parsed broker cash statement batch to OMS execution cash expectations (gap plan §5.7).
 */
@Service
public class CashReconciliationService {

    private static final ObjectMapper JSON = new ObjectMapper();

    public record Result(
            long reportId,
            long batchId,
            String status,
            int movementCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount,
            int missingInBrokerCount,
            boolean balanceMismatch,
            boolean nostroBalanceMismatch) {}

    private record PendingDetail(
            String outcome,
            String brokerMovementId,
            String executionRef,
            Long executionId,
            UUID accountId,
            BigDecimal brokerAmount,
            BigDecimal omsAmount,
            String diffJson,
            Long breakId) {}

    private final BrokerCashStatementBatchRepository batches;
    private final BrokerCashStatementMovementRepository movements;
    private final CashReconciliationLookupRepository lookup;
    private final CashReconciliationReportRepository reports;
    private final ReconciliationBreakRepository breaks;
    private final OmsConfig config;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalance;

    public CashReconciliationService(
            BrokerCashStatementBatchRepository batches,
            BrokerCashStatementMovementRepository movements,
            CashReconciliationLookupRepository lookup,
            CashReconciliationReportRepository reports,
            ReconciliationBreakRepository breaks,
            OmsConfig config,
            ObjectProvider<LedgerBalanceClient> ledgerBalance) {
        this.batches = batches;
        this.movements = movements;
        this.lookup = lookup;
        this.reports = reports;
        this.breaks = breaks;
        this.config = config;
        this.ledgerBalance = ledgerBalance;
    }

    @Transactional
    public Optional<Result> reconcile(long batchId) {
        Optional<BrokerCashStatementBatchRepository.BatchRow> batchOpt = batches.findById(batchId);
        if (batchOpt.isEmpty()) {
            return Optional.empty();
        }
        BrokerCashStatementBatchRepository.BatchRow batch = batchOpt.get();
        if (!"parsed".equals(batch.status())) {
            throw new IllegalStateException(
                    "batch " + batchId + " status must be parsed, was " + batch.status());
        }

        List<BrokerCashStatementMovementRepository.MovementRow> movementRows = movements.listByBatch(batchId);
        BigDecimal tolerance = config.getSettlement().getBrokerConfirmGrossAmountTolerance();

        int matched = 0;
        int mismatch = 0;
        int unmatched = 0;
        int missingInBroker = 0;
        List<PendingDetail> detailRows = new ArrayList<>();
        Set<String> refsInBatch = new HashSet<>();

        for (BrokerCashStatementMovementRepository.MovementRow movement : movementRows) {
            String execRef = movement.executionRef();
            if (execRef == null || execRef.isBlank()) {
                unmatched++;
                String diffJson = buildDiffJson(movement.amount(), null, "unmatched", "missing executionRef");
                long breakId = openBreak(batch, null, diffJson);
                detailRows.add(pendingDetail(
                        "unmatched", movement.brokerMovementId(), null, null, null, movement.amount(), null, diffJson, breakId));
                continue;
            }
            refsInBatch.add(execRef.trim());

            Optional<CashReconciliationLookupRepository.ExecutionCashExpectation> omsOpt =
                    lookup.findByBrokerAndVenueRef(batch.brokerId(), execRef);
            if (omsOpt.isEmpty()) {
                unmatched++;
                String diffJson = buildDiffJson(movement.amount(), null, "unmatched", "no OMS execution for executionRef");
                long breakId = openBreak(batch, null, diffJson);
                detailRows.add(pendingDetail(
                        "unmatched",
                        movement.brokerMovementId(),
                        execRef,
                        null,
                        null,
                        movement.amount(),
                        null,
                        diffJson,
                        breakId));
                continue;
            }

            CashReconciliationLookupRepository.ExecutionCashExpectation oms = omsOpt.get();
            if (!currencyMatches(batch.currency(), movement.currency(), oms.currency())) {
                mismatch++;
                String diffJson = buildDiffJson(movement.amount(), oms.expectedCashAmount(), "mismatch", "currency mismatch");
                long breakId = openBreak(batch, oms.accountId(), diffJson);
                detailRows.add(pendingDetail(
                        "mismatch",
                        movement.brokerMovementId(),
                        execRef,
                        oms.executionId(),
                        oms.accountId(),
                        movement.amount(),
                        oms.expectedCashAmount(),
                        diffJson,
                        breakId));
                continue;
            }

            if (amountsMatch(movement.amount(), oms.expectedCashAmount(), tolerance)) {
                matched++;
                detailRows.add(pendingDetail(
                        "matched",
                        movement.brokerMovementId(),
                        execRef,
                        oms.executionId(),
                        oms.accountId(),
                        movement.amount(),
                        oms.expectedCashAmount(),
                        buildDiffJson(movement.amount(), oms.expectedCashAmount(), "matched", null),
                        null));
            } else {
                mismatch++;
                String diffJson = buildDiffJson(movement.amount(), oms.expectedCashAmount(), "mismatch", "amount mismatch");
                long breakId = openBreak(batch, oms.accountId(), diffJson);
                detailRows.add(pendingDetail(
                        "mismatch",
                        movement.brokerMovementId(),
                        execRef,
                        oms.executionId(),
                        oms.accountId(),
                        movement.amount(),
                        oms.expectedCashAmount(),
                        diffJson,
                        breakId));
            }
        }

        String[] refsArray = refsInBatch.toArray(String[]::new);
        for (CashReconciliationLookupRepository.ExecutionCashExpectation missing :
                lookup.findSettledExecutionsMissingRefs(batch.brokerId(), batch.businessDate(), batch.currency(), refsArray)) {
            missingInBroker++;
            String diffJson = buildDiffJson(null, missing.expectedCashAmount(), "missing_in_broker", missing.venueExecRef());
            long breakId = openBreak(batch, missing.accountId(), diffJson);
            detailRows.add(pendingDetail(
                    "missing_in_broker",
                    null,
                    missing.venueExecRef(),
                    missing.executionId(),
                    missing.accountId(),
                    null,
                    missing.expectedCashAmount(),
                    diffJson,
                    breakId));
        }

        boolean balanceMismatch = false;
        if (batch.openingBalance() != null && batch.closingBalance() != null) {
            BigDecimal sumMovements = movementRows.stream()
                    .map(BrokerCashStatementMovementRepository.MovementRow::amount)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal computedClosing = batch.openingBalance().add(sumMovements);
            balanceMismatch = computedClosing.subtract(batch.closingBalance()).abs().compareTo(tolerance) > 0;
            if (balanceMismatch) {
                ObjectNode root = JSON.createObjectNode();
                root.put("outcome", "balance_check");
                root.put("openingBalance", batch.openingBalance().toPlainString());
                root.put("sumMovements", sumMovements.toPlainString());
                root.put("computedClosing", computedClosing.toPlainString());
                root.put("brokerClosingBalance", batch.closingBalance().toPlainString());
                String diffJson = serialize(root);
                detailRows.add(pendingDetail(
                        "balance_check", null, null, null, null, batch.closingBalance(), computedClosing, diffJson, null));
            }
        }

        boolean nostroBalanceMismatch = false;
        if (batch.closingBalance() != null && batch.currency() != null && !batch.currency().isBlank()) {
            LedgerBalanceClient client = ledgerBalance.getIfAvailable();
            if (client != null) {
                try {
                    String indicator = config.getSettlement().nostroIndicatorForCurrency(batch.currency());
                    BigDecimal ledgerNostro =
                            client.fetchAvailableBalanceByIndicator(indicator, batch.currency().trim());
                    nostroBalanceMismatch =
                            ledgerNostro.subtract(batch.closingBalance()).abs().compareTo(tolerance) > 0;
                    if (nostroBalanceMismatch) {
                        ObjectNode root = JSON.createObjectNode();
                        root.put("outcome", "nostro_balance_mismatch");
                        root.put("ledgerNostro", ledgerNostro.toPlainString());
                        root.put("brokerClosingBalance", batch.closingBalance().toPlainString());
                        root.put("nostroIndicator", indicator);
                        String diffJson = serialize(root);
                        long breakId = openBreak(batch, null, diffJson);
                        detailRows.add(pendingDetail(
                                "nostro_balance_mismatch",
                                null,
                                null,
                                null,
                                null,
                                batch.closingBalance(),
                                ledgerNostro,
                                diffJson,
                                breakId));
                    }
                } catch (LedgerBalanceClient.LedgerServiceException e) {
                    ObjectNode root = JSON.createObjectNode();
                    root.put("outcome", "nostro_balance_mismatch");
                    root.put("reason", "ledger_nostro_read_failed");
                    root.put("detail", e.getMessage());
                    detailRows.add(pendingDetail(
                            "nostro_balance_mismatch",
                            null,
                            null,
                            null,
                            null,
                            batch.closingBalance(),
                            null,
                            serialize(root),
                            null));
                }
            }
        }

        long reportId = reports.insertReport(new CashReconciliationReportRepository.ReportInsert(
                batchId,
                batch.brokerId(),
                batch.businessDate(),
                batch.currency(),
                "completed",
                movementRows.size(),
                matched,
                mismatch,
                unmatched,
                missingInBroker,
                balanceMismatch,
                nostroBalanceMismatch));

        for (PendingDetail pending : detailRows) {
            reports.insertDetail(new CashReconciliationReportRepository.DetailInsert(
                    reportId,
                    pending.outcome(),
                    pending.brokerMovementId(),
                    pending.executionRef(),
                    pending.executionId(),
                    pending.accountId(),
                    pending.brokerAmount(),
                    pending.omsAmount(),
                    pending.diffJson(),
                    pending.breakId()));
        }

        return Optional.of(new Result(
                reportId,
                batchId,
                "completed",
                movementRows.size(),
                matched,
                mismatch,
                unmatched,
                missingInBroker,
                balanceMismatch,
                nostroBalanceMismatch));
    }

    private long openBreak(
            BrokerCashStatementBatchRepository.BatchRow batch, UUID accountId, String diffJson) {
        return breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_CASH_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                null,
                null,
                accountId,
                batch.businessDate(),
                diffJson,
                "system"));
    }

    private static boolean currencyMatches(String batchCurrency, String movementCurrency, String omsCurrency) {
        String bc = batchCurrency == null ? "" : batchCurrency.trim();
        String mc = movementCurrency == null ? bc : movementCurrency.trim();
        String oc = omsCurrency == null ? "" : omsCurrency.trim();
        return bc.equalsIgnoreCase(mc) && bc.equalsIgnoreCase(oc);
    }

    private static boolean amountsMatch(BigDecimal broker, BigDecimal oms, BigDecimal tolerance) {
        if (broker == null || oms == null) {
            return false;
        }
        BigDecimal tol = tolerance == null ? BigDecimal.ZERO : tolerance;
        return broker.subtract(oms).abs().compareTo(tol) <= 0;
    }

    private static PendingDetail pendingDetail(
            String outcome,
            String brokerMovementId,
            String executionRef,
            Long executionId,
            UUID accountId,
            BigDecimal brokerAmount,
            BigDecimal omsAmount,
            String diffJson,
            Long breakId) {
        return new PendingDetail(
                outcome, brokerMovementId, executionRef, executionId, accountId, brokerAmount, omsAmount, diffJson, breakId);
    }

    private static String buildDiffJson(
            BigDecimal broker, BigDecimal oms, String outcome, String reason) {
        ObjectNode root = JSON.createObjectNode();
        root.put("outcome", outcome);
        if (reason != null) {
            root.put("reason", reason);
        }
        if (broker != null) {
            root.put("brokerAmount", broker.toPlainString());
        }
        if (oms != null) {
            root.put("omsAmount", oms.toPlainString());
        }
        if (broker != null && oms != null) {
            root.put("delta", broker.subtract(oms).toPlainString());
        }
        return serialize(root);
    }

    private static String serialize(ObjectNode root) {
        try {
            return JSON.writeValueAsString(root);
        } catch (Exception e) {
            return "{\"outcome\":\"error\"}";
        }
    }
}
