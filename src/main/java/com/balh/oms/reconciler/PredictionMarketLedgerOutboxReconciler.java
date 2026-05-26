package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerSettlementLegPoster;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.settlement.PredictionMarketLedgerOutboxRepository;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

/** Drains {@code prediction_market_ledger_outbox} after the resolution dispute window (Phase B). */
public class PredictionMarketLedgerOutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(PredictionMarketLedgerOutboxReconciler.class);

    private final PredictionMarketLedgerOutboxRepository outbox;
    private final LedgerSettlementLegPoster poster;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public PredictionMarketLedgerOutboxReconciler(
            PredictionMarketLedgerOutboxRepository outbox,
            LedgerSettlementLegPoster poster,
            OmsConfig config,
            org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.poster = poster;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.venue.resolution-ledger-outbox-reconciler-interval-ms:1000}")
    public void runOnce() {
        if (!config.getLedger().isSettlementOutboxReconcilerEnabled()) {
            return;
        }
        transactionTemplate.executeWithoutResult(
                status -> {
                    List<PredictionMarketLedgerOutboxRepository.OutboxRow> rows =
                            outbox.lockEligible(
                                    Instant.now(),
                                    config.getCorporateAction().getLedgerOutboxReconcilerBatchSize());
                    for (PredictionMarketLedgerOutboxRepository.OutboxRow row : rows) {
                        Instant attemptAt = Instant.now();
                        try {
                            poster.postPredictionMarketOutbox(row.id(), row.legKind(), row.payloadJson());
                            outbox.markPosted(row.id(), Instant.now());
                        } catch (LedgerSettlementPostingClient.LedgerSettlementPostingException e) {
                            outbox.recordAttempt(row.id(), truncate(e.getMessage()), attemptAt);
                            log.warn(
                                    "prediction_market ledger outbox post failed id={} resolution={}: {}",
                                    row.id(),
                                    row.resolutionId(),
                                    e.getMessage());
                        }
                    }
                });
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 4000 ? message : message.substring(0, 4000);
    }
}
