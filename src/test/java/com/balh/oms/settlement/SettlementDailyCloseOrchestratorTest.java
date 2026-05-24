package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class SettlementDailyCloseOrchestratorTest {

    private OmsConfig config;
    private BrokerTradeConfirmBatchLifecycleService confirmLifecycle;
    private BrokerPositionSnapshotBatchRepository positionBatches;
    private PositionReconciliationService positionReconciliation;
    private BrokerCashStatementBatchRepository cashBatches;
    private CashReconciliationService cashReconciliation;
    private SettlementDailyCloseOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getSettlement().setDailyCloseLookbackHours(24);
        config.getSettlement().setDailyCloseBatchLimit(5);
        confirmLifecycle = mock(BrokerTradeConfirmBatchLifecycleService.class);
        positionBatches = mock(BrokerPositionSnapshotBatchRepository.class);
        positionReconciliation = mock(PositionReconciliationService.class);
        cashBatches = mock(BrokerCashStatementBatchRepository.class);
        cashReconciliation = mock(CashReconciliationService.class);
        BrokerCorporateActionBatchRepository caBatches = mock(BrokerCorporateActionBatchRepository.class);
        BrokerCorporateActionApplyService caApply = mock(BrokerCorporateActionApplyService.class);
        CorporateActionReconciliationService caRecon = mock(CorporateActionReconciliationService.class);
        BrokerSettlementFailBatchRepository failBatches = mock(BrokerSettlementFailBatchRepository.class);
        BrokerSettlementFailApplyService failApply = mock(BrokerSettlementFailApplyService.class);

        when(positionBatches.listParsedWithoutReportSince(any(Instant.class), anyInt())).thenReturn(List.of(42L));
        when(cashBatches.listParsedWithoutReportSince(any(Instant.class), anyInt())).thenReturn(List.of());
        when(caBatches.listParsedSince(any(Instant.class), anyInt())).thenReturn(List.of());
        when(failBatches.listParsedWithoutApplySince(any(Instant.class), anyInt())).thenReturn(List.of());
        when(positionReconciliation.reconcile(42L))
                .thenReturn(Optional.of(new PositionReconciliationService.Result(1, 42, "completed", 1, 1, 0, 0, 0)));

        orchestrator =
                new SettlementDailyCloseOrchestrator(
                        config,
                        confirmLifecycle,
                        positionBatches,
                        positionReconciliation,
                        cashBatches,
                        cashReconciliation,
                        caBatches,
                        caApply,
                        caRecon,
                        failBatches,
                        failApply,
                        new SimpleMeterRegistry());
    }

    @Test
    void runDailyClose_reconcilesParsedPositionBatch() {
        orchestrator.runDailyClose();
        verify(confirmLifecycle).processAllParsedBatches();
        verify(positionReconciliation).reconcile(42L);
    }

    @Test
    void runDailyClose_emitsMetric() {
        var registry = new SimpleMeterRegistry();
        orchestrator =
                new SettlementDailyCloseOrchestrator(
                        config,
                        confirmLifecycle,
                        positionBatches,
                        positionReconciliation,
                        cashBatches,
                        cashReconciliation,
                        mock(BrokerCorporateActionBatchRepository.class),
                        mock(BrokerCorporateActionApplyService.class),
                        mock(CorporateActionReconciliationService.class),
                        mock(BrokerSettlementFailBatchRepository.class),
                        mock(BrokerSettlementFailApplyService.class),
                        registry);
        orchestrator.runDailyClose();
        assertThat(registry.find("oms_settlement_daily_close_total").counters()).isNotEmpty();
    }
}
