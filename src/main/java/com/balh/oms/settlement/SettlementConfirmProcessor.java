package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.PositionsRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Consumes {@code broker_settlement_confirm} rows and advances {@code TRADE} executions through
 * §12.3 until {@code settled} (BUY securities leg updates {@code positions} on the final step).
 */
@Service
public class SettlementConfirmProcessor {

    private static final Logger log = LoggerFactory.getLogger(SettlementConfirmProcessor.class);

    private static final String METRIC_PROCESSED = "oms_settlement_broker_confirm_processed_total";

    private final BrokerSettlementConfirmRepository confirms;
    private final ExecutionsRepository executions;
    private final PositionsRepository positions;
    private final OmsConfig config;
    private final Counter processedCounter;
    private SettlementConfirmProcessor self;

    public SettlementConfirmProcessor(
            BrokerSettlementConfirmRepository confirms,
            ExecutionsRepository executions,
            PositionsRepository positions,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.confirms = confirms;
        this.executions = executions;
        this.positions = positions;
        this.config = config;
        this.processedCounter = meterRegistry.counter(METRIC_PROCESSED);
    }

    @Autowired
    public void setSelf(@Lazy SettlementConfirmProcessor self) {
        this.self = self;
    }

    public int registerBrokerConfirms(List<Long> executionIds) {
        int inserted = 0;
        for (Long id : new LinkedHashSet<>(executionIds)) {
            if (id == null || id <= 0) {
                continue;
            }
            inserted += confirms.insertIgnore(id);
        }
        return inserted;
    }

    /**
     * Registers confirms then drains the queue in separate transactions (each batch pass is
     * transactional via the proxied {@link #processPendingBatch(int)}).
     */
    public void registerAndDrain(List<Long> executionIds, int maxPasses, int batchSize) {
        registerBrokerConfirms(executionIds);
        for (int i = 0; i < maxPasses; i++) {
            int n = self.processPendingBatch(batchSize);
            if (n == 0) {
                return;
            }
        }
        throw new IllegalStateException("settlement confirm queue not drained after " + maxPasses + " passes");
    }

    @Transactional
    public int processPendingBatch(int maxRows) {
        int cap = Math.min(maxRows, config.getSettlement().getBrokerConfirmReconcilerBatchSize());
        List<BrokerSettlementConfirmRepository.PendingConfirmRow> batch = confirms.lockPendingBatch(cap);
        Instant now = Instant.now();
        for (BrokerSettlementConfirmRepository.PendingConfirmRow row : batch) {
            advanceSingleExecution(row.executionId());
            confirms.markApplied(row.id(), now);
            processedCounter.increment();
        }
        return batch.size();
    }

    /**
     * Advances one §12.3 step for a {@code TRADE} execution (internal API / tests). Idempotent when
     * already terminal — returns the current terminal status.
     *
     * @return new status after the step, or {@code null} if the execution id does not exist
     * @throws IllegalArgumentException if the row is not {@code TRADE}
     */
    @Transactional
    public String advanceOneSettlementStep(long executionId) {
        var opt = executions.findSettlementRow(executionId);
        if (opt.isEmpty()) {
            return null;
        }
        var snap = opt.get();
        if (!"TRADE".equalsIgnoreCase(snap.execType())) {
            throw new IllegalArgumentException("settlement advances apply to TRADE executions only");
        }
        String cur = snap.settlementStatus().trim().toLowerCase(Locale.ROOT);
        if (SettlementStateMachine.isTerminal(cur)) {
            return cur;
        }
        String next = SettlementStateMachine.next(cur)
                .orElseThrow(() -> new IllegalStateException("no settlement transition from " + cur));
        applyTransition(snap, cur, next);
        return next;
    }

    private void advanceSingleExecution(long executionId) {
        var snapshot = executions.findSettlementRow(executionId).orElseThrow(() ->
                new IllegalStateException("execution not found for settlement: " + executionId));
        if (!"TRADE".equalsIgnoreCase(snapshot.execType())) {
            log.debug("Skipping settlement pipeline for non-TRADE execution {}", executionId);
            return;
        }
        String cur = snapshot.settlementStatus().trim().toLowerCase(Locale.ROOT);
        if (SettlementStateMachine.isTerminal(cur)) {
            return;
        }
        while (!SettlementStateMachine.isTerminal(cur)) {
            final String curState = cur;
            String next = SettlementStateMachine.next(curState)
                    .orElseThrow(() -> new IllegalStateException("no settlement transition from " + curState));
            applyTransition(snapshot, curState, next);
            cur = next;
        }
    }

    private void applyTransition(SettlementExecutionRow snapshot, String cur, String next) {
        if ("settled".equals(next) && "BUY".equalsIgnoreCase(snapshot.side())) {
            UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
            positions.recordBuySettled(
                    snapshot.accountId(),
                    snapshot.instrumentSymbol(),
                    custody,
                    snapshot.lastQuantity(),
                    snapshot.executionId());
        }
        int n = executions.updateSettlementStatusIf(snapshot.executionId(), cur, next);
        if (n != 1) {
            throw new IllegalStateException(
                    "settlement CAS failed execution=%s %s->%s"
                            .formatted(snapshot.executionId(), cur, next));
        }
    }
}
