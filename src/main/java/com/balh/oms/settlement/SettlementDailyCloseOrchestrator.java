package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Post-EOD orchestration: match trade confirms, reconcile position/cash, apply/reconcile CA and fails
 * for parsed drop-folder batches in the lookback window (gap plan Phase C exit / §8 daily recon).
 */
@Service
public class SettlementDailyCloseOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SettlementDailyCloseOrchestrator.class);
    private static final String METRIC = "oms_settlement_daily_close_total";

    private final OmsConfig config;
    private final BrokerTradeConfirmBatchLifecycleService confirmLifecycle;
    private final BrokerPositionSnapshotBatchRepository positionBatches;
    private final PositionReconciliationService positionReconciliation;
    private final BrokerCashStatementBatchRepository cashBatches;
    private final CashReconciliationService cashReconciliation;
    private final BrokerCorporateActionBatchRepository corporateActionBatches;
    private final BrokerCorporateActionApplyService corporateActionApply;
    private final CorporateActionReconciliationService corporateActionReconciliation;
    private final BrokerSettlementFailBatchRepository failBatches;
    private final BrokerSettlementFailApplyService failApply;
    private final MeterRegistry meterRegistry;

    public SettlementDailyCloseOrchestrator(
            OmsConfig config,
            BrokerTradeConfirmBatchLifecycleService confirmLifecycle,
            BrokerPositionSnapshotBatchRepository positionBatches,
            PositionReconciliationService positionReconciliation,
            BrokerCashStatementBatchRepository cashBatches,
            CashReconciliationService cashReconciliation,
            BrokerCorporateActionBatchRepository corporateActionBatches,
            BrokerCorporateActionApplyService corporateActionApply,
            CorporateActionReconciliationService corporateActionReconciliation,
            BrokerSettlementFailBatchRepository failBatches,
            BrokerSettlementFailApplyService failApply,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.confirmLifecycle = confirmLifecycle;
        this.positionBatches = positionBatches;
        this.positionReconciliation = positionReconciliation;
        this.cashBatches = cashBatches;
        this.cashReconciliation = cashReconciliation;
        this.corporateActionBatches = corporateActionBatches;
        this.corporateActionApply = corporateActionApply;
        this.corporateActionReconciliation = corporateActionReconciliation;
        this.failBatches = failBatches;
        this.failApply = failApply;
        this.meterRegistry = meterRegistry;
    }

    public void runDailyClose() {
        var st = config.getSettlement();
        Instant since = Instant.now().minus(st.getDailyCloseLookbackHours(), ChronoUnit.HOURS);
        int limit = st.getDailyCloseBatchLimit();

        matchTradeConfirms();
        reconcilePositionBatches(since, limit);
        reconcileCashBatches(since, limit);
        applyAndReconcileCorporateActions(since, limit);
        applySettlementFails(since, limit);
    }

    private void matchTradeConfirms() {
        try {
            int n = confirmLifecycle.processAllParsedBatches();
            record("trade_confirm_match", n > 0 ? "applied" : "noop");
            if (n > 0) {
                log.info("Daily close: matched {} broker confirm batch(es)", n);
            }
        } catch (Exception e) {
            record("trade_confirm_match", "error");
            log.warn("Daily close: trade confirm match failed", e);
        }
    }

    private void reconcilePositionBatches(Instant since, int limit) {
        List<Long> ids = positionBatches.listParsedWithoutReportSince(since, limit);
        for (Long batchId : ids) {
            try {
                positionReconciliation.reconcile(batchId).ifPresent(r -> {
                    record("position_reconcile", r.mismatchCount() > 0 ? "break" : "ok");
                    log.info(
                            "Daily close: position batch {} reconciled matched={} mismatch={}",
                            batchId,
                            r.matchedCount(),
                            r.mismatchCount());
                });
            } catch (Exception e) {
                record("position_reconcile", "error");
                log.warn("Daily close: position reconcile failed batchId={}", batchId, e);
            }
        }
        if (ids.isEmpty()) {
            record("position_reconcile", "noop");
        }
    }

    private void reconcileCashBatches(Instant since, int limit) {
        List<Long> ids = cashBatches.listParsedWithoutReportSince(since, limit);
        for (Long batchId : ids) {
            try {
                cashReconciliation.reconcile(batchId).ifPresent(r -> {
                    String outcome = r.nostroBalanceMismatch() || r.mismatchCount() > 0 ? "break" : "ok";
                    record("cash_reconcile", outcome);
                    log.info(
                            "Daily close: cash batch {} reconciled matched={} mismatch={} nostroMismatch={}",
                            batchId,
                            r.matchedCount(),
                            r.mismatchCount(),
                            r.nostroBalanceMismatch());
                });
            } catch (Exception e) {
                record("cash_reconcile", "error");
                log.warn("Daily close: cash reconcile failed batchId={}", batchId, e);
            }
        }
        if (ids.isEmpty()) {
            record("cash_reconcile", "noop");
        }
    }

    private void applyAndReconcileCorporateActions(Instant since, int limit) {
        List<Long> ids = corporateActionBatches.listParsedSince(since, limit);
        for (Long batchId : ids) {
            try {
                corporateActionApply.apply(batchId);
                corporateActionReconciliation.reconcile(batchId).ifPresent(r -> {
                    record("ca_reconcile", r.mismatchCount() > 0 ? "break" : "ok");
                    log.info(
                            "Daily close: CA batch {} reconciled matched={} mismatch={}",
                            batchId,
                            r.matchedCount(),
                            r.mismatchCount());
                });
            } catch (Exception e) {
                record("ca_reconcile", "error");
                log.warn("Daily close: CA apply/reconcile failed batchId={}", batchId, e);
            }
        }
        if (ids.isEmpty()) {
            record("ca_reconcile", "noop");
        }
    }

    private void applySettlementFails(Instant since, int limit) {
        List<Long> ids = failBatches.listParsedWithoutApplySince(since, limit);
        for (Long batchId : ids) {
            try {
                failApply.apply(batchId).ifPresent(r -> {
                    record("fail_apply", "applied".equals(r.status()) ? "ok" : "partial");
                    log.info("Daily close: fail batch {} apply status={}", batchId, r.status());
                });
            } catch (Exception e) {
                record("fail_apply", "error");
                log.warn("Daily close: fail apply failed batchId={}", batchId, e);
            }
        }
        if (ids.isEmpty()) {
            record("fail_apply", "noop");
        }
    }

    private void record(String step, String outcome) {
        meterRegistry.counter(METRIC, List.of(Tag.of("step", step), Tag.of("outcome", outcome))).increment();
    }
}
