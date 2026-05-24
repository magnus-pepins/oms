package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrderFeeSnapshot;
import com.balh.oms.persistence.OrderFeeSnapshotRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for the V40/V41 fee-snapshot + cross-currency cash plumbing in
 * {@link SettlementConfirmProcessor#enqueueSettlementLegs(SettlementExecutionRow)}.
 *
 * <p>Drives one {@code settling → settled} transition via
 * {@link SettlementConfirmProcessor#advanceOneSettlementStep(long)} with mocked
 * repositories, then captures the payloads passed to
 * {@link LedgerSettlementOutboxRepository#insertIgnore(long, String, String, String)} and
 * asserts that:
 *
 * <ul>
 *   <li>No snapshot → single {@code cash} leg + {@code fee} leg derived from the
 *       default {@code StockCommissionCalculator} schedule
 *       ({@code feeSource=default-schedule}, {@code feeTier=default});</li>
 *   <li>Snapshot with USD-only cash currency → single {@code cash} leg + {@code fee}
 *       leg uses the pinned amount/tier/source ({@code feeSource=snapshot-<source>});</li>
 *   <li>Snapshot with cross-currency cash + non-null {@code cashAmount}/{@code fxRate}
 *       → two cash legs ({@code cash-base} + {@code cash-quote}) plus the fee leg,
 *       and never a single {@code cash} leg;</li>
 *   <li>Snapshot with cross-currency cash but null {@code cashAmount}/{@code fxRate}
 *       → defensive fallback to a single {@code cash} leg so the trade still settles
 *       (operator sees the warning and the pinned fee tier).</li>
 * </ul>
 *
 * <p>This is the regression net for the BFF-side {@code order_fee_snapshots}
 * contract: changes to the OMS payload shape that drop {@code feeSource} /
 * {@code feeTier} / cross-currency leg routing will fail this test, not the
 * customer's bill.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SettlementConfirmProcessorFeeSnapshotTest {

    private static final long EXECUTION_ID = 4242L;
    private static final UUID ORDER_ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID ACCOUNT_ID = UUID.fromString("22222222-2222-4222-8222-222222222222");
    private static final String SETTLING = "settling";
    private static final String SETTLED = "settled";
    private static final String SETTLED_STATUS = "settled";

    @Mock private BrokerSettlementConfirmRepository confirms;
    @Mock private ExecutionsRepository executions;
    @Mock private PositionsRepository positions;
    @Mock private LedgerSettlementOutboxRepository outbox;
    @Mock private OrderFeeSnapshotRepository feeSnapshots;

    private OmsConfig config;
    private ObjectMapper objectMapper;
    private SettlementConfirmProcessor processor;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        // Outbox emission is the whole point of this test path.
        config.getLedger().setSettlementOutboxEnabled(true);
        // Settled transition needs a custody UUID (validated by setter); defaults
        // to a well-formed sentinel when blank, which is fine for this unit.
        config.getSettlement().setDefaultCustodyAccountId("a0000001-0000-4000-8000-000000000001");
        config.getSettlement().setDefaultInstrumentMarket("US");
        config.getSettlement().setDefaultCashCurrency("USD");

        objectMapper = new ObjectMapper();
        processor = new SettlementConfirmProcessor(
                confirms, executions, positions, outbox, feeSnapshots,
                objectMapper, config, new SimpleMeterRegistry(),
                new IskSettlementMetadataService(
                        mock(com.balh.oms.settlement.OmsAccountTaxWrapperRepository.class)));
        // SettlementConfirmProcessor.applyTransition is called directly from
        // advanceOneSettlementStep (no @Transactional proxy needed here), so
        // self is irrelevant — but advanceOneSettlementStep doesn't dispatch
        // through self, only processPendingBatch does. We leave self null on purpose.

        when(executions.updateSettlementStatusIf(eq(EXECUTION_ID), eq(SETTLING), eq(SETTLED)))
                .thenReturn(1);
    }

    @Test
    void noSnapshotFallsBackToDefaultScheduleFee() throws Exception {
        // Notional 1 share × $100 = $100 → 0.25% = $0.25 → clamped up to min $1.00.
        // The fee leg should therefore record $1.00 with feeSource=default-schedule.
        var row = settlingRow(new BigDecimal("1"), new BigDecimal("100.00"));
        when(executions.findSettlementRow(EXECUTION_ID)).thenReturn(Optional.of(row));
        when(feeSnapshots.findByOrderId(ORDER_ID)).thenReturn(Optional.empty());

        String next = processor.advanceOneSettlementStep(EXECUTION_ID);

        assertThat(next).isEqualTo(SETTLED);
        var cash = captureLegPayload(LedgerSettlementOutboxRepository.LEG_CASH);
        assertThat(cash.get("tradeCurrency").asText()).isEqualTo("USD");
        assertThat(cash.get("cashCurrency").asText()).isEqualTo("USD");
        assertThat(cash.get("quantity").asText()).isEqualTo("1");
        assertThat(cash.get("price").asText()).isEqualTo("100.00");

        var fee = captureLegPayload(LedgerSettlementOutboxRepository.LEG_FEE);
        assertThat(new BigDecimal(fee.get("feeAmount").asText())).isEqualByComparingTo("1.00");
        assertThat(fee.get("feeCurrency").asText()).isEqualTo("USD");
        assertThat(fee.get("feeBalanceIndicator").asText()).isEqualTo("@Fees-USD");
        assertThat(fee.get("feeTier").asText()).isEqualTo("default");
        assertThat(fee.get("feeSource").asText()).isEqualTo("default-schedule");

        // And we must NOT have emitted Phase 2 cross-currency legs.
        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_BASE), anyString());
        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_QUOTE), anyString());
    }

    @Test
    void singleCurrencySnapshotPinsFeeWithoutTouchingCrossCurrencyLegs() throws Exception {
        var row = settlingRow(new BigDecimal("3"), new BigDecimal("50.00"));
        when(executions.findSettlementRow(EXECUTION_ID)).thenReturn(Optional.of(row));
        when(feeSnapshots.findByOrderId(ORDER_ID))
                .thenReturn(Optional.of(snapshot("0.77", "USD", "@Fees-USD",
                        "professional", "override", null, null, null)));

        String next = processor.advanceOneSettlementStep(EXECUTION_ID);

        assertThat(next).isEqualTo(SETTLED);
        var cash = captureLegPayload(LedgerSettlementOutboxRepository.LEG_CASH);
        assertThat(cash.get("cashCurrency").asText()).isEqualTo("USD");
        assertThat(cash.get("tradeCurrency").asText()).isEqualTo("USD");

        var fee = captureLegPayload(LedgerSettlementOutboxRepository.LEG_FEE);
        assertThat(new BigDecimal(fee.get("feeAmount").asText())).isEqualByComparingTo("0.77");
        assertThat(fee.get("feeTier").asText()).isEqualTo("professional");
        // feeSource is prefixed with "snapshot-" so operators can grep the
        // outbox payload and immediately see the BFF pin won the race.
        assertThat(fee.get("feeSource").asText()).isEqualTo("snapshot-override");

        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_BASE), anyString());
        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_QUOTE), anyString());
    }

    @Test
    void crossCurrencySnapshotEmitsBaseAndQuoteLegs() throws Exception {
        // EUR-funded customer buying USD AAPL: cashCurrency=EUR, tradeCurrency=USD.
        // BFF pre-computed cashAmount + fxRate at quote time; OMS replays them
        // verbatim into the cash-base / cash-quote payloads so the poster does
        // not have to re-derive FX at settlement time.
        var row = settlingRow(new BigDecimal("2"), new BigDecimal("150.00"));
        when(executions.findSettlementRow(EXECUTION_ID)).thenReturn(Optional.of(row));
        when(feeSnapshots.findByOrderId(ORDER_ID))
                .thenReturn(Optional.of(snapshot("0.50", "EUR", "@Fees-EUR",
                        "individual", "override", "EUR", "278.10", "0.927")));

        String next = processor.advanceOneSettlementStep(EXECUTION_ID);

        assertThat(next).isEqualTo(SETTLED);

        var base = captureLegPayload(LedgerSettlementOutboxRepository.LEG_CASH_BASE);
        assertThat(base.get("cashCurrency").asText()).isEqualTo("EUR");
        assertThat(base.get("tradeCurrency").asText()).isEqualTo("USD");
        assertThat(new BigDecimal(base.get("cashAmount").asText())).isEqualByComparingTo("278.10");
        assertThat(new BigDecimal(base.get("fxRate").asText())).isEqualByComparingTo("0.927");

        var quote = captureLegPayload(LedgerSettlementOutboxRepository.LEG_CASH_QUOTE);
        assertThat(quote.get("cashCurrency").asText()).isEqualTo("EUR");
        assertThat(quote.get("tradeCurrency").asText()).isEqualTo("USD");
        assertThat(new BigDecimal(quote.get("cashAmount").asText())).isEqualByComparingTo("278.10");
        assertThat(new BigDecimal(quote.get("fxRate").asText())).isEqualByComparingTo("0.927");

        var fee = captureLegPayload(LedgerSettlementOutboxRepository.LEG_FEE);
        assertThat(fee.get("feeCurrency").asText()).isEqualTo("EUR");
        assertThat(new BigDecimal(fee.get("feeAmount").asText())).isEqualByComparingTo("0.50");

        // Cross-currency path must NOT also emit a single-currency cash leg —
        // that would double-debit the customer.
        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH), anyString());
    }

    @Test
    void crossCurrencySnapshotMissingCashAmountFallsBackToSingleLegForSafety() throws Exception {
        // Operator pinned a non-USD cashCurrency but the BFF forgot to compute
        // cashAmount/fxRate. The trade must still settle (in tradeCurrency) to
        // not strand the customer — the warning log + audit row are the only
        // operator-visible signal that someone needs to fix the BFF.
        var row = settlingRow(new BigDecimal("1"), new BigDecimal("100.00"));
        when(executions.findSettlementRow(EXECUTION_ID)).thenReturn(Optional.of(row));
        when(feeSnapshots.findByOrderId(ORDER_ID))
                .thenReturn(Optional.of(snapshot("1.00", "USD", "@Fees-USD",
                        "individual", "tier", "EUR", null, null)));

        String next = processor.advanceOneSettlementStep(EXECUTION_ID);

        assertThat(next).isEqualTo(SETTLED);

        var cash = captureLegPayload(LedgerSettlementOutboxRepository.LEG_CASH);
        // Fallback forces cashCurrency=tradeCurrency to keep poster invariants.
        assertThat(cash.get("cashCurrency").asText()).isEqualTo("USD");
        assertThat(cash.get("tradeCurrency").asText()).isEqualTo("USD");

        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_BASE), anyString());
        verify(outbox, never()).insertIgnore(
                anyLong(), anyString(),
                eq(LedgerSettlementOutboxRepository.LEG_CASH_QUOTE), anyString());
    }

    @Test
    void snapshotLookupSkippedWhenOrderIdMissing() throws Exception {
        // Defensive: pre-V40 execution rows may have a null orderId. We must
        // not call findByOrderId(null) — that would NPE in the JdbcTemplate
        // setter. The default-schedule fallback applies instead.
        var row = new SettlementExecutionRow(
                EXECUTION_ID, /* orderId */ null, SETTLING, "TRADE",
                new BigDecimal("1"), new BigDecimal("100.00"),
                ACCOUNT_ID, "AAPL", "BUY",
                /* sellPositionFromPendingBuy */ null, /* sellPositionFromSettled */ null);
        when(executions.findSettlementRow(EXECUTION_ID)).thenReturn(Optional.of(row));

        String next = processor.advanceOneSettlementStep(EXECUTION_ID);

        assertThat(next).isEqualTo(SETTLED);
        verify(feeSnapshots, never()).findByOrderId(any());

        var fee = captureLegPayload(LedgerSettlementOutboxRepository.LEG_FEE);
        assertThat(fee.get("feeSource").asText()).isEqualTo("default-schedule");
    }

    private SettlementExecutionRow settlingRow(BigDecimal qty, BigDecimal price) {
        return new SettlementExecutionRow(
                EXECUTION_ID, ORDER_ID, SETTLING, "TRADE",
                qty, price,
                ACCOUNT_ID, "AAPL", "BUY",
                /* sellPositionFromPendingBuy */ null, /* sellPositionFromSettled */ null);
    }

    private OrderFeeSnapshot snapshot(
            String feeAmount,
            String feeCurrency,
            String feeBalanceIndicator,
            String feeTier,
            String feeSource,
            String cashCurrency,
            String cashAmount,
            String fxRate) {
        return new OrderFeeSnapshot(
                ORDER_ID,
                new BigDecimal(feeAmount),
                feeCurrency,
                feeBalanceIndicator,
                feeTier,
                feeSource,
                /* feeScheduleId */ null,
                /* userFeeOverrideId */ null,
                cashCurrency,
                cashAmount == null ? null : new BigDecimal(cashAmount),
                fxRate == null ? null : new BigDecimal(fxRate),
                Instant.now());
    }

    /**
     * Captures the {@code payload_json} string for the single outbox insert
     * matching {@code legKind} and parses it back into a JsonNode for assertion.
     * Fails the test if zero or more than one matching insert was recorded.
     */
    private JsonNode captureLegPayload(String legKind) throws Exception {
        var captor = ArgumentCaptor.forClass(String.class);
        verify(outbox, times(1)).insertIgnore(
                eq(EXECUTION_ID), eq(SETTLED_STATUS), eq(legKind), captor.capture());
        return objectMapper.readTree(captor.getValue());
    }
}
