package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * Consumes {@code broker_settlement_confirm} rows and advances {@code TRADE} executions through
 * §12.3 until {@code settled} (BUY securities leg updates {@code positions} on the final step).
 */
@Service
public class SettlementConfirmProcessor {

    private static final Logger log = LoggerFactory.getLogger(SettlementConfirmProcessor.class);

    private static final String METRIC_PROCESSED = "oms_settlement_broker_confirm_processed_total";
    private static final String METRIC_MARK_FAILED = "oms_settlement_trade_mark_failed_total";

    private final BrokerSettlementConfirmRepository confirms;
    private final ExecutionsRepository executions;
    private final PositionsRepository positions;
    private final LedgerSettlementOutboxRepository ledgerSettlementOutbox;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final Counter processedCounter;
    private SettlementConfirmProcessor self;

    public SettlementConfirmProcessor(
            BrokerSettlementConfirmRepository confirms,
            ExecutionsRepository executions,
            PositionsRepository positions,
            LedgerSettlementOutboxRepository ledgerSettlementOutbox,
            ObjectMapper objectMapper,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.confirms = confirms;
        this.executions = executions;
        this.positions = positions;
        this.ledgerSettlementOutbox = ledgerSettlementOutbox;
        this.objectMapper = objectMapper;
        this.config = config;
        this.meterRegistry = meterRegistry;
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
     * Inserts a pending {@code broker_settlement_confirm} row when the execution exists and is {@code TRADE}.
     * Duplicate queue rows are ignored ({@code ON CONFLICT DO NOTHING}).
     *
     * @return {@code 1} if a new row was inserted, {@code 0} if a pending or applied row already existed for this
     *     execution
     * @throws IllegalStateException if the execution id is unknown or not {@code TRADE}
     */
    public int enqueueBrokerSettlementConfirmForTradeOrThrow(long executionId) {
        var snap = executions.findSettlementRow(executionId).orElseThrow(() ->
                new IllegalStateException("execution not found for broker confirm enqueue: " + executionId));
        if (!"TRADE".equalsIgnoreCase(snap.execType())) {
            throw new IllegalStateException(
                    "broker confirm enqueue applies to TRADE executions only: " + executionId);
        }
        return confirms.insertIgnore(executionId);
    }

    /**
     * Removes pending {@code broker_settlement_confirm} rows for a {@code TRADE} execution without changing
     * {@code settlement_status} (operator correction when a confirm was queued in error).
     */
    @Transactional
    public ClearPendingBrokerConfirmResult clearPendingBrokerConfirmsForTradeOrThrow(long executionId) {
        var opt = executions.findSettlementRow(executionId);
        if (opt.isEmpty()) {
            return ClearPendingBrokerConfirmResult.NOT_FOUND;
        }
        var snap = opt.get();
        if (!"TRADE".equalsIgnoreCase(snap.execType())) {
            return ClearPendingBrokerConfirmResult.NOT_TRADE;
        }
        confirms.deletePendingForExecution(executionId);
        return ClearPendingBrokerConfirmResult.APPLIED;
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
        if ("settled".equals(next)) {
            UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
            if ("BUY".equalsIgnoreCase(snapshot.side())) {
                positions.recordBuySettled(
                        snapshot.accountId(),
                        snapshot.instrumentSymbol(),
                        custody,
                        snapshot.lastQuantity(),
                        snapshot.executionId());
            } else if ("SELL".equalsIgnoreCase(snapshot.side())) {
                positions.recordSellSettled(
                        snapshot.accountId(),
                        snapshot.instrumentSymbol(),
                        custody,
                        snapshot.lastQuantity(),
                        snapshot.executionId());
            }
        }
        int n = executions.updateSettlementStatusIf(snapshot.executionId(), cur, next);
        if (n != 1) {
            throw new IllegalStateException(
                    "settlement CAS failed execution=%s %s->%s"
                            .formatted(snapshot.executionId(), cur, next));
        }
        if (config.getLedger().isSettlementOutboxEnabled() && "settled".equals(next)) {
            try {
                ObjectNode p = objectMapper.createObjectNode();
                p.put("schemaVersion", 1);
                p.put("event", "SETTLEMENT_SETTLED");
                p.put("executionId", snapshot.executionId());
                p.put("side", snapshot.side());
                p.put("instrumentSymbol", snapshot.instrumentSymbol());
                p.put("quantity", snapshot.lastQuantity().toPlainString());
                int inserted = ledgerSettlementOutbox.insertIgnore(
                        snapshot.executionId(), "settled", objectMapper.writeValueAsString(p));
                log.debug(
                        "ledger settlement outbox enqueue executionId={} inserted={}",
                        snapshot.executionId(),
                        inserted);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("ledger settlement outbox payload failed", e);
            }
        }
    }

    /**
     * Resolves broker fixture rows to {@code executions.id} (same rules as {@link #registerBrokerConfirmsFromFixture}
     * but does not insert {@code broker_settlement_confirm}).
     */
    public BrokerFixtureResolutionOutcome resolveBrokerFixtureRows(List<BrokerFixtureRow> rows, int maxRows) {
        if (rows == null || rows.isEmpty()) {
            return new BrokerFixtureResolutionOutcome(List.of(), 0, 0);
        }
        int skippedInvalid = 0;
        int skippedUnresolved = 0;
        List<Long> resolved = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            if (i >= maxRows) {
                skippedInvalid++;
                continue;
            }
            BrokerFixtureRow row = rows.get(i);
            if (invalidFixtureShape(row)) {
                skippedInvalid++;
                continue;
            }
            Optional<Long> idOpt = resolveFixtureRow(row);
            if (idOpt.isEmpty()) {
                skippedUnresolved++;
                continue;
            }
            resolved.add(idOpt.get());
        }
        return new BrokerFixtureResolutionOutcome(resolved, skippedInvalid, skippedUnresolved);
    }

    /**
     * Resolves broker fixture rows to {@code executions.id} and registers {@code broker_settlement_confirm} rows.
     */
    public BrokerFixtureImportResult registerBrokerConfirmsFromFixture(List<BrokerFixtureRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new BrokerFixtureImportResult(0, 0, 0);
        }
        int max = config.getSettlement().getBrokerConfirmHttpMaxExecutionIds();
        BrokerFixtureResolutionOutcome outcome = resolveBrokerFixtureRows(rows, max);
        int inserted = registerBrokerConfirms(outcome.resolvedExecutionIds());
        return new BrokerFixtureImportResult(
                inserted, outcome.skippedUnresolvedRows(), outcome.skippedInvalidRows());
    }

    /**
     * Marks a {@code TRADE} execution {@code failed} (§12.7); removes pending broker confirms; unwinds
     * {@code positions} for the trade fill when {@link PositionsRepository#revertPositionForMarkTradeFailed}
     * applies (BUY always; SELL when {@code executions.sell_position_from_*} were stored at fill time).
     */
    @Transactional
    public MarkTradeFailedResult markTradeFailed(long executionId) {
        var opt = executions.findSettlementRow(executionId);
        if (opt.isEmpty()) {
            noteMarkFailed(MarkTradeFailedResult.NOT_FOUND);
            return MarkTradeFailedResult.NOT_FOUND;
        }
        var snap = opt.get();
        if (!"TRADE".equalsIgnoreCase(snap.execType())) {
            noteMarkFailed(MarkTradeFailedResult.NOT_TRADE);
            return MarkTradeFailedResult.NOT_TRADE;
        }
        String cur = snap.settlementStatus().trim().toLowerCase(Locale.ROOT);
        if ("failed".equals(cur)) {
            noteMarkFailed(MarkTradeFailedResult.ALREADY_FAILED);
            return MarkTradeFailedResult.ALREADY_FAILED;
        }
        if ("settled".equals(cur)) {
            noteMarkFailed(MarkTradeFailedResult.ALREADY_SETTLED);
            return MarkTradeFailedResult.ALREADY_SETTLED;
        }
        confirms.deletePendingForExecution(executionId);
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        positions.revertPositionForMarkTradeFailed(snap, custody);
        int n = executions.failTradeSettlementToFailed(executionId);
        if (n != 1) {
            throw new IllegalStateException("mark settlement failed: unexpected CAS execution=" + executionId);
        }
        noteMarkFailed(MarkTradeFailedResult.APPLIED);
        return MarkTradeFailedResult.APPLIED;
    }

    private static boolean invalidFixtureShape(BrokerFixtureRow row) {
        boolean hasExec = row.executionId() != null && row.executionId() > 0;
        boolean hasPair =
                row.accountId() != null && row.venueExecRef() != null && !row.venueExecRef().isBlank();
        return !hasExec && !hasPair;
    }

    private Optional<Long> resolveFixtureRow(BrokerFixtureRow row) {
        if (row.executionId() != null && row.executionId() > 0) {
            return executions.findSettlementRow(row.executionId()).map(SettlementExecutionRow::executionId);
        }
        return executions.findTradeExecutionIdByAccountAndVenueRef(row.accountId(), row.venueExecRef());
    }

    private void noteMarkFailed(MarkTradeFailedResult r) {
        meterRegistry.counter(METRIC_MARK_FAILED, List.of(Tag.of("result", r.name()))).increment();
    }
}
