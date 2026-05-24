package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Applies parsed broker settlement fail rows to {@code execution_settlement_lot} and, on full
 * fails, {@link SettlementConfirmProcessor#markTradeFailed(long)} (gap plan §5.8 Slice 10b).
 */
@Service
public class BrokerSettlementFailApplyService {

    private static final String METRIC_APPLY = "oms_settlement_fail_file_apply_total";
    private static final String LOT_FAILED = "failed";
    private static final String LOT_PENDING = "pending";

    public record Result(
            long batchId,
            String status,
            int failRowCount,
            int appliedCount,
            int fullFailCount,
            int partialFailCount,
            int unmatchedCount,
            int quantityMismatchCount,
            int skippedAlreadyApplied) {}

    private final BrokerSettlementFailBatchRepository batches;
    private final BrokerSettlementFailRowRepository failRows;
    private final BrokerSettlementFailLookupRepository lookup;
    private final ExecutionSettlementLotRepository lots;
    private final ReconciliationBreakRepository breaks;
    private final SettlementConfirmProcessor settlementProcessor;
    private final SettlementFailPenaltyBookingService penaltyBooking;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public BrokerSettlementFailApplyService(
            BrokerSettlementFailBatchRepository batches,
            BrokerSettlementFailRowRepository failRows,
            BrokerSettlementFailLookupRepository lookup,
            ExecutionSettlementLotRepository lots,
            ReconciliationBreakRepository breaks,
            SettlementConfirmProcessor settlementProcessor,
            SettlementFailPenaltyBookingService penaltyBooking,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.batches = batches;
        this.failRows = failRows;
        this.lookup = lookup;
        this.lots = lots;
        this.breaks = breaks;
        this.settlementProcessor = settlementProcessor;
        this.penaltyBooking = penaltyBooking;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public Optional<Result> apply(long batchId) {
        Optional<BrokerSettlementFailBatchRepository.BatchRow> batchOpt = batches.findById(batchId);
        if (batchOpt.isEmpty()) {
            return Optional.empty();
        }
        BrokerSettlementFailBatchRepository.BatchRow batch = batchOpt.get();
        if (!"parsed".equals(batch.status()) && !"applied".equals(batch.status())) {
            throw new IllegalStateException("batch not parsed: status=" + batch.status());
        }

        List<BrokerSettlementFailRowRepository.FailRow> rows = failRows.listByBatch(batchId);
        int applied = 0;
        int fullFail = 0;
        int partialFail = 0;
        int unmatched = 0;
        int quantityMismatch = 0;
        int skipped = 0;

        for (BrokerSettlementFailRowRepository.FailRow row : rows) {
            if (row.appliedAt() != null) {
                skipped++;
                continue;
            }
            Optional<BrokerSettlementFailLookupRepository.TradeTarget> target =
                    lookup.findTradeByBrokerVenueRef(batch.brokerId(), row.executionRef());
            if (target.isEmpty()) {
                long breakId = openUnmatchedBreak(batch, row);
                failRows.markApplyResult(row.id(), null, null, "execution not found");
                unmatched++;
                recordApply("unmatched");
                continue;
            }
            BrokerSettlementFailLookupRepository.TradeTarget trade = target.get();
            BigDecimal tradeQty = nz(trade.lastQuantity());
            BigDecimal failedQty = nz(row.failedQuantity());
            if (failedQty.compareTo(tradeQty) > 0) {
                long breakId = openQuantityMismatchBreak(batch, row, trade, failedQty, tradeQty);
                failRows.markApplyResult(
                        row.id(), trade.executionId(), null, "failedQuantity exceeds trade quantity");
                quantityMismatch++;
                recordApply("quantity_mismatch");
                continue;
            }

            long failedLotId = lots.insert(new ExecutionSettlementLotRepository.InsertCommand(
                    trade.executionId(),
                    failedQty,
                    row.intendedSettlementDate(),
                    null,
                    LOT_FAILED,
                    row.failReason(),
                    row.brokerFailId(),
                    row.id()));

            if (failedQty.compareTo(tradeQty) >= 0) {
                settlementProcessor.markTradeFailed(trade.executionId());
                failRows.markApplyResult(row.id(), trade.executionId(), failedLotId, null);
                penaltyBooking.enqueueIfPresent(row, trade.executionId(), trade.accountId());
                fullFail++;
                applied++;
                recordApply("full_fail");
            } else {
                BigDecimal pendingQty = tradeQty.subtract(failedQty);
                lots.insert(new ExecutionSettlementLotRepository.InsertCommand(
                        trade.executionId(),
                        pendingQty,
                        row.intendedSettlementDate(),
                        null,
                        LOT_PENDING,
                        null,
                        row.brokerFailId() + ":pending",
                        row.id()));
                failRows.markApplyResult(row.id(), trade.executionId(), failedLotId, null);
                penaltyBooking.enqueueIfPresent(row, trade.executionId(), trade.accountId());
                partialFail++;
                applied++;
                recordApply("partial_fail");
            }
        }

        if (!"applied".equals(batch.status())) {
            batches.updateStatus(batchId, "applied", batch.failCount(), null);
        }

        return Optional.of(new Result(
                batchId,
                "applied",
                rows.size(),
                applied,
                fullFail,
                partialFail,
                unmatched,
                quantityMismatch,
                skipped));
    }

    private long openUnmatchedBreak(
            BrokerSettlementFailBatchRepository.BatchRow batch, BrokerSettlementFailRowRepository.FailRow row) {
        ObjectNode diff = objectMapper.createObjectNode();
        diff.put("brokerFailId", row.brokerFailId());
        diff.put("executionRef", row.executionRef());
        diff.put("brokerId", batch.brokerId());
        diff.put("reason", "execution not found for broker venue ref");
        return breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_UNMATCHED,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                null,
                null,
                null,
                batch.businessDate(),
                diff.toString(),
                "broker-settlement-fail-apply"));
    }

    private long openQuantityMismatchBreak(
            BrokerSettlementFailBatchRepository.BatchRow batch,
            BrokerSettlementFailRowRepository.FailRow row,
            BrokerSettlementFailLookupRepository.TradeTarget trade,
            BigDecimal failedQty,
            BigDecimal tradeQty) {
        ObjectNode diff = objectMapper.createObjectNode();
        diff.put("brokerFailId", row.brokerFailId());
        diff.put("executionRef", row.executionRef());
        diff.put("executionId", trade.executionId());
        diff.put("failedQuantity", failedQty.toPlainString());
        diff.put("tradeQuantity", tradeQty.toPlainString());
        return breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_QUANTITY_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                null,
                trade.executionId(),
                trade.accountId(),
                batch.businessDate(),
                diff.toString(),
                "broker-settlement-fail-apply"));
    }

    private void recordApply(String outcome) {
        meterRegistry.counter(METRIC_APPLY, List.of(Tag.of("outcome", outcome))).increment();
    }

    private static BigDecimal nz(BigDecimal v) {
        return v == null ? BigDecimal.ZERO : v;
    }
}
