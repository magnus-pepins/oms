package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class SettlementAutoStepSchedulerTest {

    @Mock private SettlementConfirmProcessor processor;
    @Mock private SettlementExecutionsRepository executionsRepository;
    @Mock private ExecutionsRepository executions;

    private OmsConfig config;
    private SettlementAutoStepScheduler scheduler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getSettlement().setAutoStepSchedulerEnabled(true);
        config.getSettlement().setAutoStepSchedulerMaxAdvanceFailures(3);
        scheduler =
                new SettlementAutoStepScheduler(
                        config, processor, executionsRepository, executions, new SimpleMeterRegistry());
    }

    @Test
    void tick_disabled_doesNotQueryRepository() {
        config.getSettlement().setAutoStepSchedulerEnabled(false);

        scheduler.tick();

        verifyNoInteractions(executionsRepository);
        verifyNoInteractions(processor);
    }

    @Test
    void tick_passesConfiguredMaxExecutionAgeToRepository() {
        config.getSettlement().setAutoStepSchedulerMaxExecutionAgeSeconds(1800);
        config.getSettlement().setAutoStepSchedulerMaxAdvanceFailures(7);
        config.getSettlement().setAutoStepSchedulerBatchSize(15);
        when(executionsRepository.findNonTerminalTradeIds(1800L, 7, 15)).thenReturn(List.of());

        scheduler.tick();

        verify(executionsRepository).findNonTerminalTradeIds(1800L, 7, 15);
        verifyNoInteractions(processor);
    }

    @Test
    void tick_successfulAdvance_clearsFailureCounter() {
        when(executionsRepository.findNonTerminalTradeIds(anyLong(), eq(3), anyInt()))
                .thenReturn(List.of(42L));
        when(processor.advanceOneSettlementStep(42L)).thenReturn("matched");

        scheduler.tick();

        verify(executions).clearSettlementAutoStepFailures(42L);
        verify(executions, never()).recordSettlementAutoStepFailure(anyLong(), anyString());
    }

    @Test
    void tick_repeatedFailures_triggersPoisonPill() {
        when(executionsRepository.findNonTerminalTradeIds(anyLong(), eq(3), anyInt()))
                .thenReturn(List.of(99L));
        when(processor.advanceOneSettlementStep(99L)).thenThrow(new IllegalStateException("boom"));
        when(executions.recordSettlementAutoStepFailure(99L, "boom")).thenReturn(Optional.of(3));
        when(processor.markTradeFailed(99L)).thenReturn(MarkTradeFailedResult.APPLIED);

        scheduler.tick();

        verify(processor).markTradeFailed(99L);
    }
}
