package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Drives {@code broker_confirm_batch} from {@code parsed} through {@code matching} to
 * {@code applied} after {@link BrokerTradeConfirmMatcher} resolves pending confirms in a batch.
 */
@Service
public class BrokerTradeConfirmBatchLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(BrokerTradeConfirmBatchLifecycleService.class);

    private final BrokerConfirmBatchRepository batches;
    private final BrokerTradeConfirmRepository confirms;
    private final BrokerTradeConfirmMatcher matcher;
    private final OmsConfig config;

    public BrokerTradeConfirmBatchLifecycleService(
            BrokerConfirmBatchRepository batches,
            BrokerTradeConfirmRepository confirms,
            BrokerTradeConfirmMatcher matcher,
            OmsConfig config) {
        this.batches = batches;
        this.confirms = confirms;
        this.matcher = matcher;
        this.config = config;
    }

    public record BatchMatchSummary(
            long batchId,
            String finalStatus,
            int processedRows,
            int matchedRows,
            int breakRows,
            List<BrokerTradeConfirmMatcher.MatchResult> results) {}

    /**
     * Match all pending confirms for {@code batchId}, then advance batch counters and status.
     *
     * @return empty when the batch is unknown or not in {@code parsed}/{@code matching}
     */
    @Transactional
    public Optional<BatchMatchSummary> processBatchMatches(long batchId) {
        Optional<BrokerConfirmBatchRepository.BatchRow> batch = batches.findById(batchId);
        if (batch.isEmpty()) {
            return Optional.empty();
        }
        String status = batch.get().status();
        if (!"parsed".equals(status) && !"matching".equals(status)) {
            return Optional.empty();
        }
        batches.updateStatus(batchId, "matching", null, null);

        int cap = config.getSettlement().getBrokerConfirmReconcilerBatchSize();
        int totalProcessed = 0;
        int matchedInRun = 0;
        int breakInRun = 0;
        List<BrokerTradeConfirmMatcher.MatchResult> lastResults = List.of();

        while (confirms.countPendingForBatch(batchId) > 0) {
            lastResults = matcher.processPendingForBatch(batchId, cap);
            if (lastResults.isEmpty()) {
                break;
            }
            for (BrokerTradeConfirmMatcher.MatchResult r : lastResults) {
                totalProcessed++;
                switch (r.outcome()) {
                    case MATCHED -> matchedInRun++;
                    case MISMATCH, UNRESOLVED -> breakInRun++;
                    case ALREADY_DECIDED -> { /* counted in prior tick */ }
                }
            }
        }

        BrokerTradeConfirmRepository.BatchMatchCounts counts = confirms.countMatchOutcomesByBatch(batchId);
        batches.updateMatchOutcomes(batchId, counts.matched(), counts.breakRows());
        batches.updateStatus(batchId, "applied", null, null);
        log.info(
                "broker confirm batch applied id={} matched={} breaks={} processedThisRun={}",
                batchId,
                counts.matched(),
                counts.breakRows(),
                totalProcessed);
        return Optional.of(new BatchMatchSummary(
                batchId, "applied", totalProcessed, counts.matched(), counts.breakRows(), lastResults));
    }

    /** Process every batch still in {@code parsed} (scheduler entry point). */
    public int processAllParsedBatches() {
        int cap = config.getSettlement().getBrokerTradeConfirmMatcherSchedulerBatchSize();
        List<Long> ids = batches.findIdsByStatus("parsed", cap);
        int applied = 0;
        for (Long id : ids) {
            if (processBatchMatches(id).isPresent()) {
                applied++;
            }
        }
        return applied;
    }
}
