package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementTimelineResponse;
import com.balh.oms.persistence.SettlementExecutionDetailRow;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SettlementTimelineServiceTest {

    private static final Instant T0 = Instant.parse("2026-05-20T10:00:00Z");
    private static final Instant T1 = Instant.parse("2026-05-20T10:05:00Z");
    private static final Instant T2 = Instant.parse("2026-05-20T10:10:00Z");
    private static final Instant T3 = Instant.parse("2026-05-20T10:15:00Z");
    private static final LocalDate TRADE_DATE = LocalDate.parse("2026-05-20");
    private static final LocalDate EXPECTED_SETTLEMENT_DATE = LocalDate.parse("2026-05-22");

    @Mock private com.balh.oms.persistence.SettlementExecutionsRepository executions;
    @Mock private BrokerSettlementConfirmRepository brokerConfirms;
    @Mock private LedgerSettlementOutboxRepository outbox;

    private SettlementTimelineService service;

    @BeforeEach
    void setUp() {
        service = new SettlementTimelineService(executions, brokerConfirms, outbox);
    }

    @Test
    void loadTimeline_missingExecution_returnsEmpty() {
        when(executions.findById(404L)).thenReturn(Optional.empty());

        assertThat(service.loadTimeline(404L)).isEmpty();
    }

    @Test
    void loadTimeline_executedOnly_includesExecutedPhase() {
        when(executions.findById(1L)).thenReturn(Optional.of(execRow(1L, "executed", T0)));
        when(brokerConfirms.findByExecution(1L)).thenReturn(Optional.empty());
        when(outbox.findByExecution(1L)).thenReturn(List.of());

        SettlementTimelineResponse timeline =
                service.loadTimeline(1L).orElseThrow();

        assertThat(timeline.executionId()).isEqualTo(1L);
        assertThat(timeline.currentSettlementStatus()).isEqualTo("executed");
        assertThat(timeline.tradeDate()).isEqualTo(TRADE_DATE);
        assertThat(timeline.expectedSettlementDate()).isEqualTo(EXPECTED_SETTLEMENT_DATE);
        assertThat(timeline.phases()).extracting(SettlementTimelineResponse.Phase::phase)
                .containsExactly("executed");
        assertThat(timeline.ledgerLegs()).isEmpty();
    }

    @Test
    void loadTimeline_confirmedAndSettled_includesMatchedConfirmedAndSettledPhases() {
        when(executions.findById(2L)).thenReturn(Optional.of(execRow(2L, "settled", T0)));
        when(brokerConfirms.findByExecution(2L))
                .thenReturn(
                        Optional.of(
                                new BrokerSettlementConfirmRepository.ConfirmTimelineRow(10L, 2L, T1, T2)));
        when(outbox.findByExecution(2L))
                .thenReturn(
                        List.of(
                                new LedgerSettlementOutboxRepository.OutboxTimelineRow(
                                        20L,
                                        2L,
                                        "settled",
                                        LedgerSettlementOutboxRepository.LEG_CASH,
                                        "{}",
                                        T3,
                                        T3,
                                        1,
                                        null,
                                        T3,
                                        null,
                                        null)));

        SettlementTimelineResponse timeline =
                service.loadTimeline(2L).orElseThrow();

        assertThat(timeline.phases()).extracting(SettlementTimelineResponse.Phase::phase)
                .containsExactly("executed", "matched", "confirmed", "settled");
        assertThat(timeline.ledgerLegs()).hasSize(1);
        var leg = timeline.ledgerLegs().getFirst();
        assertThat(leg.legKind()).isEqualTo(LedgerSettlementOutboxRepository.LEG_CASH);
        assertThat(leg.attempts()).isEqualTo(1);
        assertThat(leg.postedAt()).isEqualTo(T3);
        assertThat(leg.lastErrorText()).isNull();
        assertThat(leg.skippedAt()).isNull();
    }

    @Test
    void loadTimeline_failingLeg_exposesAttemptsAndLastError() {
        when(executions.findById(4L)).thenReturn(Optional.of(execRow(4L, "settled", T0)));
        when(brokerConfirms.findByExecution(4L)).thenReturn(Optional.empty());
        when(outbox.findByExecution(4L))
                .thenReturn(
                        List.of(
                                new LedgerSettlementOutboxRepository.OutboxTimelineRow(
                                        40L,
                                        4L,
                                        "settled",
                                        LedgerSettlementOutboxRepository.LEG_CASH,
                                        "{}",
                                        T1,
                                        null,
                                        3,
                                        "ledger /transactions HTTP 503: unavailable",
                                        T3,
                                        null,
                                        null)));

        SettlementTimelineResponse timeline =
                service.loadTimeline(4L).orElseThrow();

        var leg = timeline.ledgerLegs().getFirst();
        assertThat(leg.attempts()).isEqualTo(3);
        assertThat(leg.lastErrorText()).contains("HTTP 503");
        assertThat(leg.lastAttemptAt()).isEqualTo(T3);
        assertThat(leg.postedAt()).isNull();
        assertThat(leg.skippedAt()).isNull();
        assertThat(leg.skipReason()).isNull();
    }

    @Test
    void loadTimeline_skippedLeg_exposesSkipReason() {
        when(executions.findById(5L)).thenReturn(Optional.of(execRow(5L, "settled", T0)));
        when(brokerConfirms.findByExecution(5L)).thenReturn(Optional.empty());
        when(outbox.findByExecution(5L))
                .thenReturn(
                        List.of(
                                new LedgerSettlementOutboxRepository.OutboxTimelineRow(
                                        50L,
                                        5L,
                                        "settled",
                                        LedgerSettlementOutboxRepository.LEG_CASH,
                                        "{}",
                                        T1,
                                        null,
                                        5,
                                        "customer balance not found in Ledger: indicator=inv-…",
                                        T2,
                                        T3,
                                        "unfunded_balance")));

        SettlementTimelineResponse timeline =
                service.loadTimeline(5L).orElseThrow();

        var leg = timeline.ledgerLegs().getFirst();
        assertThat(leg.skippedAt()).isEqualTo(T3);
        assertThat(leg.skipReason()).isEqualTo("unfunded_balance");
        assertThat(leg.attempts()).isEqualTo(5);
        assertThat(leg.postedAt()).isNull();
    }

    @Test
    void loadTimeline_legacyExecutionWithoutDates_exposesNullDateFields() {
        when(executions.findById(6L))
                .thenReturn(Optional.of(execRowWithoutDates(6L, "executed", T0)));
        when(brokerConfirms.findByExecution(6L)).thenReturn(Optional.empty());
        when(outbox.findByExecution(6L)).thenReturn(List.of());

        SettlementTimelineResponse timeline =
                service.loadTimeline(6L).orElseThrow();

        assertThat(timeline.tradeDate()).isNull();
        assertThat(timeline.expectedSettlementDate()).isNull();
    }

    @Test
    void loadTimeline_terminalFailed_stillReturnsExecutedPhase() {
        when(executions.findById(3L)).thenReturn(Optional.of(execRow(3L, "failed", T0)));
        when(brokerConfirms.findByExecution(3L)).thenReturn(Optional.empty());
        when(outbox.findByExecution(3L)).thenReturn(List.of());

        SettlementTimelineResponse timeline =
                service.loadTimeline(3L).orElseThrow();

        assertThat(timeline.currentSettlementStatus()).isEqualTo("failed");
        assertThat(timeline.phases()).extracting(SettlementTimelineResponse.Phase::phase)
                .containsExactly("executed");
    }

    private static SettlementExecutionDetailRow execRow(long id, String settlementStatus, Instant createdAt) {
        return execRow(id, settlementStatus, createdAt, TRADE_DATE, EXPECTED_SETTLEMENT_DATE);
    }

    private static SettlementExecutionDetailRow execRowWithoutDates(
            long id, String settlementStatus, Instant createdAt) {
        return execRow(id, settlementStatus, createdAt, null, null);
    }

    private static SettlementExecutionDetailRow execRow(
            long id,
            String settlementStatus,
            Instant createdAt,
            LocalDate tradeDate,
            LocalDate expectedSettlementDate) {
        return new SettlementExecutionDetailRow(
                id,
                UUID.randomUUID(),
                UUID.randomUUID(),
                "SIM",
                createdAt,
                "vref-" + id,
                BigDecimal.ONE,
                BigDecimal.TEN,
                BigDecimal.ZERO,
                BigDecimal.ONE,
                "TRADE",
                settlementStatus,
                createdAt,
                "FILLED",
                "BUY",
                "AAPL",
                tradeDate,
                expectedSettlementDate,
                "{}");
    }
}
