package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Drains {@code ledger_settlement_outbox} to Ledger HTTP after {@link com.balh.oms.settlement.SettlementConfirmProcessor}
 * enqueues rows (gated by {@code oms.ledger.settlement-outbox-reconciler-enabled}).
 *
 * <p>Deploy with {@code OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true} on a single JVM
 * (intended: {@code oms-postgres-projector}) so ticks do not race across ingress / fix-egress replicas.
 */
public class LedgerSettlementOutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(LedgerSettlementOutboxReconciler.class);

    private static final String METRIC_PUBLISHED = "oms_ledger_settlement_outbox_published_total";
    private static final String METRIC_FAILED = "oms_ledger_settlement_outbox_failed_total";
    /**
     * Skipped (not failed) outbox posts. Tag {@code reason} mirrors
     * {@link LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason} so an SRE
     * panel can chart operator-actionable skips separately from infra failures.
     */
    private static final String METRIC_SKIPPED = "oms_ledger_settlement_outbox_skipped_total";
    private static final String METRIC_SKIP_TOMBSTONE = "oms_ledger_settlement_outbox_skip_tombstoned_total";
    private static final String TIMER_LEDGER_SETTLEMENT_POST = "oms.ledger.settlement.outbox.post";

    private final LedgerSettlementOutboxRepository outbox;
    private final LedgerSettlementPostingClient postingClient;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;
    private final Timer ledgerSettlementOutboxPostTimer;

    public LedgerSettlementOutboxReconciler(
            LedgerSettlementOutboxRepository outbox,
            LedgerSettlementPostingClient postingClient,
            OmsConfig config,
            org.springframework.transaction.PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry) {
        this.outbox = outbox;
        this.postingClient = postingClient;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.meterRegistry = meterRegistry;
        this.ledgerSettlementOutboxPostTimer =
                Timer.builder(TIMER_LEDGER_SETTLEMENT_POST)
                        .description("Ledger HTTP settlement outbox post plus markPosted in one DB txn")
                        .publishPercentileHistogram()
                        .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${oms.ledger.settlement-outbox-reconciler-interval-ms:500}")
    public void runOnce() {
        if (!config.getLedger().isSettlementOutboxReconcilerEnabled()) {
            return;
        }
        transactionTemplate.executeWithoutResult(status -> {
            var ledger = config.getLedger();
            Instant olderThan =
                    Instant.now().minus(ledger.getSettlementOutboxReconcilerAgeMs(), ChronoUnit.MILLIS);
            var rows = outbox.lockUnpostedOlderThan(olderThan, ledger.getSettlementOutboxReconcilerBatchSize());
            int skipAfterAttempts = ledger.getSettlementOutboxSkipAfterAttempts();
            for (LedgerSettlementOutboxRepository.OutboxRow row : rows) {
                Timer.Sample sample = Timer.start(meterRegistry);
                Instant attemptAt = Instant.now();
                String lastError = null;
                LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason skipReason = null;
                try {
                    postingClient.postSettlementOutbox(
                            row.id(),
                            row.executionId(),
                            row.toSettlementStatus(),
                            row.legKind(),
                            row.payloadJson());
                    outbox.markPosted(row.id(), Instant.now());
                    meterRegistry.counter(METRIC_PUBLISHED).increment();
                } catch (LedgerSettlementPostingClient.LedgerSettlementPostingException e) {
                    lastError = e.getMessage();
                    if (e.reason()
                            == LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason
                                    .SKIPPED_UNFUNDED_BALANCE) {
                        skipReason =
                                LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason
                                        .SKIPPED_UNFUNDED_BALANCE;
                        meterRegistry
                                .counter(METRIC_SKIPPED, List.of(Tag.of("reason", "unfunded_balance")))
                                .increment();
                        log.info(
                                "ledger settlement outbox skipped id={} executionId={} leg={} reason=unfunded_balance: {}",
                                row.id(),
                                row.executionId(),
                                row.legKind(),
                                e.getMessage());
                    } else if (e.reason()
                            == LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason
                                    .SKIPPED_INDICATOR_NOT_FOUND) {
                        skipReason =
                                LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason
                                        .SKIPPED_INDICATOR_NOT_FOUND;
                        meterRegistry
                                .counter(METRIC_SKIPPED, List.of(Tag.of("reason", "indicator_not_found")))
                                .increment();
                        log.warn(
                                "ledger settlement outbox skipped id={} executionId={} leg={} reason=indicator_not_found: {}",
                                row.id(),
                                row.executionId(),
                                row.legKind(),
                                e.getMessage());
                    } else {
                        meterRegistry.counter(METRIC_FAILED).increment();
                        log.warn(
                                "ledger settlement outbox deliver failed id={} executionId={} leg={}: {}",
                                row.id(),
                                row.executionId(),
                                row.legKind(),
                                e.getMessage());
                    }
                } catch (RuntimeException e) {
                    lastError = e.getMessage();
                    meterRegistry.counter(METRIC_FAILED).increment();
                    log.warn(
                            "ledger settlement outbox deliver failed id={} executionId={} leg={}",
                            row.id(),
                            row.executionId(),
                            row.legKind(),
                            e);
                } finally {
                    outbox.recordAttempt(row.id(), attemptAt, lastError);
                    if (skipReason != null && row.attempts() + 1 >= skipAfterAttempts) {
                        String tombstoneReason =
                                skipReason == LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason
                                                .SKIPPED_UNFUNDED_BALANCE
                                        ? "unfunded_balance"
                                        : "indicator_not_found";
                        if (outbox.markSkipped(row.id(), attemptAt, tombstoneReason) == 1) {
                            meterRegistry
                                    .counter(
                                            METRIC_SKIP_TOMBSTONE,
                                            List.of(Tag.of("reason", tombstoneReason)))
                                    .increment();
                            log.warn(
                                    "ledger settlement outbox tombstoned id={} executionId={} leg={} attempts={} reason={}",
                                    row.id(),
                                    row.executionId(),
                                    row.legKind(),
                                    row.attempts() + 1,
                                    tombstoneReason);
                        }
                    }
                    sample.stop(ledgerSettlementOutboxPostTimer);
                }
            }
        });
    }
}
