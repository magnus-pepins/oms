package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrderFeeSnapshot;
import com.balh.oms.persistence.OrderFeeSnapshotRepository;
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
    private final OrderFeeSnapshotRepository orderFeeSnapshots;
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
            OrderFeeSnapshotRepository orderFeeSnapshots,
            ObjectMapper objectMapper,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.confirms = confirms;
        this.executions = executions;
        this.positions = positions;
        this.ledgerSettlementOutbox = ledgerSettlementOutbox;
        this.orderFeeSnapshots = orderFeeSnapshots;
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
            enqueueSettlementLegs(snapshot);
        }
    }

    /**
     * Writes one outbox row per Ledger leg for a settled TRADE row, so cash and
     * fee retry independently (V39 unique key includes leg_kind).
     *
     * <p>Phase 1 (single-currency): customer's cash currency == instrument's trade currency,
     * so the cash leg is a single Ledger transaction. Phase 2 splits cash into
     * cash-base + cash-quote when they differ (mirrors {@code FxHedgeService}'s
     * {@code @FX-Suspense-<ccy>} pattern).
     *
     * <p>The instrument's market is currently defaulted to {@code US} — see
     * {@link com.balh.oms.config.OmsConfig.Settlement#getDefaultInstrumentMarket()}.
     * Once an instrument reference table lands we'll look it up by symbol.
     */
    private void enqueueSettlementLegs(SettlementExecutionRow snapshot) {
        if (snapshot.lastPrice() == null) {
            log.warn(
                    "ledger settlement outbox skipped: last_price is null executionId={}",
                    snapshot.executionId());
            return;
        }
        var settlementCfg = config.getSettlement();
        String market = settlementCfg.getDefaultInstrumentMarket();
        StockCommissionCalculator.Schedule schedule =
                StockCommissionCalculator.defaultScheduleFor(market);
        String tradeCurrency = schedule.currency();
        String cashCurrency = settlementCfg.getDefaultCashCurrency();
        java.math.BigDecimal notional =
                StockCommissionCalculator.notional(snapshot.lastQuantity(), snapshot.lastPrice());

        // BFF-pinned commission wins over StockCommissionCalculator's default schedule.
        // See V40 migration header for why this exists (tier / per-user override pricing
        // computed in resolveStockFee.ts on the customer-frontend side must match the
        // amount actually moved to @Fees-<ccy> at settlement). Snapshot absence is
        // expected for orders accepted before the BFF was upgraded → fall back silently.
        Optional<OrderFeeSnapshot> pinned =
                snapshot.orderId() == null
                        ? Optional.empty()
                        : orderFeeSnapshots.findByOrderId(snapshot.orderId());
        java.math.BigDecimal feeAmount;
        String feeCurrency;
        String feeBalanceIndicator;
        String feeTier;
        String feeSource;
        if (pinned.isPresent()) {
            OrderFeeSnapshot p = pinned.get();
            feeAmount = p.feeAmount();
            feeCurrency = p.feeCurrency();
            feeBalanceIndicator = p.feeBalanceIndicator();
            feeTier = p.feeTier();
            feeSource = "snapshot-" + p.feeSource();
        } else {
            feeAmount = StockCommissionCalculator.feeFor(schedule, notional);
            feeCurrency = schedule.currency();
            feeBalanceIndicator = schedule.feeBalanceIndicator();
            feeTier = "default";
            feeSource = "default-schedule";
        }

        try {
            // Cash leg payload (Phase 1: assumes cashCcy == tradeCcy and writes a single
            // 'cash' row; Phase 2 will branch to 'cash-base' + 'cash-quote' here).
            ObjectNode cash = objectMapper.createObjectNode();
            cash.put("schemaVersion", 2);
            cash.put("event", "SETTLEMENT_SETTLED");
            cash.put("leg", LedgerSettlementOutboxRepository.LEG_CASH);
            cash.put("executionId", snapshot.executionId());
            cash.put("accountId", snapshot.accountId().toString());
            cash.put("side", snapshot.side());
            cash.put("instrumentSymbol", snapshot.instrumentSymbol());
            cash.put("market", market);
            cash.put("quantity", snapshot.lastQuantity().toPlainString());
            cash.put("price", snapshot.lastPrice().toPlainString());
            cash.put("tradeCurrency", tradeCurrency);
            cash.put("cashCurrency", cashCurrency);
            cash.put("notional", notional.toPlainString());
            cash.put("settledAt", Instant.now().toString());
            int cashInserted = ledgerSettlementOutbox.insertIgnore(
                    snapshot.executionId(),
                    "settled",
                    LedgerSettlementOutboxRepository.LEG_CASH,
                    objectMapper.writeValueAsString(cash));

            int feeInserted = 0;
            if (feeAmount.signum() > 0) {
                ObjectNode fee = objectMapper.createObjectNode();
                fee.put("schemaVersion", 2);
                fee.put("event", "SETTLEMENT_SETTLED");
                fee.put("leg", LedgerSettlementOutboxRepository.LEG_FEE);
                fee.put("executionId", snapshot.executionId());
                fee.put("accountId", snapshot.accountId().toString());
                fee.put("side", snapshot.side());
                fee.put("instrumentSymbol", snapshot.instrumentSymbol());
                fee.put("market", market);
                fee.put("feeAmount", feeAmount.toPlainString());
                fee.put("feeCurrency", feeCurrency);
                fee.put("feeBalanceIndicator", feeBalanceIndicator);
                fee.put("feeTier", feeTier);
                fee.put("feeSource", feeSource);
                fee.put("notional", notional.toPlainString());
                fee.put("settledAt", Instant.now().toString());
                feeInserted = ledgerSettlementOutbox.insertIgnore(
                        snapshot.executionId(),
                        "settled",
                        LedgerSettlementOutboxRepository.LEG_FEE,
                        objectMapper.writeValueAsString(fee));
            }
            log.debug(
                    "ledger settlement outbox enqueue executionId={} cashInserted={} feeInserted={}",
                    snapshot.executionId(),
                    cashInserted,
                    feeInserted);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("ledger settlement outbox payload failed", e);
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
