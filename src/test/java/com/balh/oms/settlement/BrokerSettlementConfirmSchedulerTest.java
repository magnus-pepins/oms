package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrokerSettlementConfirmSchedulerTest {

    @Mock private SettlementConfirmProcessor processor;

    private OmsConfig config;
    private BrokerSettlementConfirmScheduler scheduler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        scheduler = new BrokerSettlementConfirmScheduler(config, processor);
    }

    @Test
    void drainPendingBrokerConfirms_disabled_skipsProcessor() {
        config.getSettlement().setBrokerConfirmReconcilerEnabled(false);

        scheduler.drainPendingBrokerConfirms();

        verify(processor, never()).processPendingBatch(anyInt());
    }

    @Test
    void drainPendingBrokerConfirms_enabled_drainsConfiguredBatchSize() {
        config.getSettlement().setBrokerConfirmReconcilerEnabled(true);
        config.getSettlement().setBrokerConfirmReconcilerBatchSize(25);
        when(processor.processPendingBatch(25)).thenReturn(2);

        scheduler.drainPendingBrokerConfirms();

        verify(processor).processPendingBatch(25);
    }

    @Test
    void drainPendingBrokerConfirms_processorFailure_doesNotPropagate() {
        config.getSettlement().setBrokerConfirmReconcilerEnabled(true);
        when(processor.processPendingBatch(anyInt())).thenThrow(new RuntimeException("db down"));

        scheduler.drainPendingBrokerConfirms();

        verify(processor).processPendingBatch(anyInt());
    }
}
