package com.balh.oms.settlement;

import com.balh.oms.ingress.SettlementTimelineResponse;
import com.balh.oms.persistence.SettlementExecutionDetailRow;
import java.math.BigDecimal;
import java.time.Instant;
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
                                        T3)));

        SettlementTimelineResponse timeline =
                service.loadTimeline(2L).orElseThrow();

        assertThat(timeline.phases()).extracting(SettlementTimelineResponse.Phase::phase)
                .containsExactly("executed", "matched", "confirmed", "settled");
        assertThat(timeline.ledgerLegs()).hasSize(1);
        assertThat(timeline.ledgerLegs().getFirst().legKind())
                .isEqualTo(LedgerSettlementOutboxRepository.LEG_CASH);
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
                "{}");
    }
}
