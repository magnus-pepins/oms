package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Drains {@code ledger_settlement_outbox} to Ledger HTTP after {@link com.balh.oms.settlement.SettlementConfirmProcessor}
 * enqueues rows (gated by {@code oms.ledger.settlement-outbox-reconciler-enabled}).
 */
public class LedgerSettlementOutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(LedgerSettlementOutboxReconciler.class);

    private static final String METRIC_PUBLISHED = "oms_ledger_settlement_outbox_published_total";
    private static final String METRIC_FAILED = "oms_ledger_settlement_outbox_failed_total";
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
            for (LedgerSettlementOutboxRepository.OutboxRow row : rows) {
                Timer.Sample sample = Timer.start(meterRegistry);
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
                    meterRegistry.counter(METRIC_FAILED).increment();
                    log.warn(
                            "ledger settlement outbox deliver failed id={} executionId={} leg={}: {}",
                            row.id(),
                            row.executionId(),
                            row.legKind(),
                            e.getMessage());
                } catch (RuntimeException e) {
                    meterRegistry.counter(METRIC_FAILED).increment();
                    log.warn(
                            "ledger settlement outbox deliver failed id={} executionId={} leg={}",
                            row.id(),
                            row.executionId(),
                            row.legKind(),
                            e);
                } finally {
                    sample.stop(ledgerSettlementOutboxPostTimer);
                }
            }
        });
    }
}
