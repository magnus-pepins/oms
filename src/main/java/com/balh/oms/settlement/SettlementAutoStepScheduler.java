package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Demo / dev auto-driver of the §12.3 settlement state machine.
 *
 * <p><strong>Why this exists.</strong> In production, TRADE executions move from
 * {@code executed} through {@code matched → confirmed → settling → settled} when the broker's
 * end-of-day settlement file is ingested (CSV/JSON via
 * {@code POST /internal/v1/settlement/file-import} → {@link SettlementConfirmProcessor}). When
 * the broker pipe is the {@link com.balh.oms.fix.it.FixRoundTripAcceptorApplication} simulator,
 * no file ever arrives and every execution stays at {@code executed} forever. beard-admin's
 * {@code SettlementExecutionsExplorer} therefore renders an all-yellow list, which makes the
 * settlement story un-demonstrable.
 *
 * <p>This scheduler advances each non-terminal TRADE execution one step per tick. With the
 * default 5-second cadence a fresh fill walks {@code executed → matched → confirmed → settling
 * → settled} over ~20 seconds, exactly the kind of visible progression that lets the demo show
 * the state machine working without forging broker fixture rows.
 *
 * <p><strong>Production safety.</strong> Off by default
 * ({@code oms.settlement.auto-step-scheduler-enabled=false}). Only flip on in environments
 * where the FIX counterparty is the simulator. Once a real broker pipe is wired up, this MUST
 * stay off — otherwise it races the broker file's authoritative transitions and corrupts
 * settlement state.
 *
 * <p>Phase B of the 2026-05-18 Wed demo follow-up; see
 * {@code system-documentation/plans/oms-ui-implementation-plan.md} §2.5 for the broader
 * customer-frontend ↔ OMS settlement story.
 */
@Component
public class SettlementAutoStepScheduler {

    private static final Logger log = LoggerFactory.getLogger(SettlementAutoStepScheduler.class);
    private static final String METRIC_TICKS = "oms.settlement.auto_step_scheduler.ticks_total";
    private static final String METRIC_TRANSITIONS = "oms.settlement.auto_step_scheduler.transitions_total";
    private static final String METRIC_FAILURES = "oms.settlement.auto_step_scheduler.failures_total";
    private static final String METRIC_POISON_PILL = "oms.settlement.auto_step_scheduler.poison_pill_total";

    private final OmsConfig config;
    private final SettlementConfirmProcessor processor;
    private final SettlementExecutionsRepository executionsRepository;
    private final ExecutionsRepository executions;
    private final Counter ticksCounter;
    private final Counter transitionsCounter;
    private final Counter failuresCounter;

    private final Counter poisonPillCounter;

    public SettlementAutoStepScheduler(
            OmsConfig config,
            SettlementConfirmProcessor processor,
            SettlementExecutionsRepository executionsRepository,
            ExecutionsRepository executions,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.processor = processor;
        this.executionsRepository = executionsRepository;
        this.executions = executions;
        this.ticksCounter = meterRegistry.counter(METRIC_TICKS);
        this.transitionsCounter = meterRegistry.counter(METRIC_TRANSITIONS);
        this.failuresCounter = meterRegistry.counter(METRIC_FAILURES);
        this.poisonPillCounter = meterRegistry.counter(METRIC_POISON_PILL);
    }

    @Scheduled(fixedDelayString = "${oms.settlement.auto-step-scheduler-interval-ms:5000}")
    public void tick() {
        var settlement = config.getSettlement();
        // DEBUG so operators can confirm the scheduler is firing without flooding
        // pm2 logs every interval. Promote to INFO temporarily if a deploy looks
        // dormant — see follow-up section in oms-ui-implementation-plan.md.
        if (log.isDebugEnabled()) {
            log.debug("auto-step scheduler tick: enabled={} maxAgeSec={} batch={}",
                    settlement.isAutoStepSchedulerEnabled(),
                    settlement.getAutoStepSchedulerMaxExecutionAgeSeconds(),
                    settlement.getAutoStepSchedulerBatchSize());
        }
        if (!settlement.isAutoStepSchedulerEnabled()) {
            return;
        }
        ticksCounter.increment();
        List<Long> ids;
        try {
            ids = executionsRepository.findNonTerminalTradeIds(
                    settlement.getAutoStepSchedulerMaxExecutionAgeSeconds(),
                    settlement.getAutoStepSchedulerMaxAdvanceFailures(),
                    settlement.getAutoStepSchedulerBatchSize());
        } catch (Exception e) {
            log.warn("auto-step scheduler: failed to query non-terminal executions", e);
            failuresCounter.increment();
            return;
        }
        if (ids.isEmpty()) {
            return;
        }
        for (Long id : ids) {
            try {
                String next = processor.advanceOneSettlementStep(id);
                if (next == null) {
                    // Row vanished between query and apply (race); benign.
                    continue;
                }
                executions.clearSettlementAutoStepFailures(id);
                transitionsCounter.increment();
                if (log.isDebugEnabled()) {
                    log.debug("auto-step scheduler: executionId={} -> {}", id, next);
                }
            } catch (IllegalArgumentException e) {
                // Not a TRADE execution (cancel/reject). Filter guarantees this shouldn't fire,
                // but log if it does so we can fix the query.
                log.warn("auto-step scheduler: non-TRADE execution slipped through filter, id={}", id);
                failuresCounter.increment();
            } catch (Exception e) {
                failuresCounter.increment();
                int maxFailures = settlement.getAutoStepSchedulerMaxAdvanceFailures();
                var failureCount =
                        executions.recordSettlementAutoStepFailure(id, e.getMessage()).orElse(0);
                if (failureCount >= maxFailures) {
                    MarkTradeFailedResult r = processor.markTradeFailed(id);
                    poisonPillCounter.increment();
                    log.warn(
                            "auto-step scheduler: poison pill executionId={} failures={} markTradeFailed={}",
                            id,
                            failureCount,
                            r);
                } else {
                    log.warn(
                            "auto-step scheduler: advance failed for executionId={} failures={}/{}",
                            id,
                            failureCount,
                            maxFailures,
                            e);
                }
            }
        }
    }
}
