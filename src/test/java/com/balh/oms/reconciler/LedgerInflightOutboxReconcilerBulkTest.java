package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LedgerInflightOutboxReconcilerBulkTest {

    private OmsConfig config;
    private LedgerInflightOutboxRepository outbox;
    private LedgerInflightBulkDispatcher bulkDispatcher;
    private LedgerInflightReservationClient reservationClient;
    private SimpleMeterRegistry meterRegistry;
    private LedgerInflightOutboxReconciler reconciler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getLedger().setEnabled(true);
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);
        config.getLedger().setInflightOutboxBulkEnabled(true);
        config.getLedger().setInflightOutboxReconcilerAgeMs(0);
        config.getLedger().setInflightOutboxReconcilerBatchSize(50);

        outbox = mock(LedgerInflightOutboxRepository.class);
        bulkDispatcher = mock(LedgerInflightBulkDispatcher.class);
        reservationClient = mock(LedgerInflightReservationClient.class);
        meterRegistry = new SimpleMeterRegistry();

        ObjectProvider<LedgerInflightReservationClient> reservationProvider = mock(ObjectProvider.class);
        when(reservationProvider.getIfAvailable()).thenReturn(reservationClient);
        ObjectProvider<LedgerInflightBulkDispatcher> bulkProvider = mock(ObjectProvider.class);
        when(bulkProvider.getIfAvailable()).thenReturn(bulkDispatcher);

        reconciler =
                new LedgerInflightOutboxReconciler(
                        outbox,
                        reservationProvider,
                        bulkProvider,
                        config,
                        new ObjectMapper(),
                        meterRegistry,
                        new NoopTransactionManager());
    }

    @Test
    void bulkEnabled_dispatchesBatchAndMarksPublished() throws Exception {
        UUID orderA = UUID.randomUUID();
        UUID orderB = UUID.randomUUID();
        when(outbox.fetchPendingOlderThan(any(), anyInt()))
                .thenReturn(
                        List.of(
                                row(1L, orderA, "{\"ledgerBalanceId\":\"bal-a\",\"holdAmount\":\"10\"}"),
                                row(2L, orderB, "{\"ledgerBalanceId\":\"bal-b\",\"holdAmount\":\"20\"}")));
        when(bulkDispatcher.dispatch(any()))
                .thenReturn(new LedgerInflightBulkDispatcher.Result(2, 2, Set.of()));

        reconciler.runOnce();

        verify(bulkDispatcher, times(1)).dispatch(any());
        verify(reservationClient, never()).placeBuyFundsHold(any(), any(), any());
        verify(outbox, times(1)).markPublished(eq(1L), any(Instant.class));
        verify(outbox, times(1)).markPublished(eq(2L), any(Instant.class));
        assertThat(meterRegistry.counter("oms_ledger_inflight_outbox_bulk_published_total").count())
                .isEqualTo(2.0);
    }

    @Test
    void bulkPartialFailure_marksOnlyFailedRows() throws Exception {
        UUID lucky = UUID.randomUUID();
        UUID unlucky = UUID.randomUUID();
        when(outbox.fetchPendingOlderThan(any(), anyInt()))
                .thenReturn(
                        List.of(
                                row(10L, lucky, "{\"ledgerBalanceId\":\"bal-1\",\"holdAmount\":\"1\"}"),
                                row(11L, unlucky, "{\"ledgerBalanceId\":\"bal-2\",\"holdAmount\":\"2\"}")));
        when(bulkDispatcher.dispatch(any()))
                .thenReturn(new LedgerInflightBulkDispatcher.Result(2, 1, Set.of(unlucky)));

        reconciler.runOnce();

        verify(outbox, times(1)).markPublished(eq(10L), any(Instant.class));
        verify(outbox, times(1)).markFailed(eq(11L), eq("ledger_bulk_partial_failure"), any(Instant.class));
    }

    private static LedgerInflightOutboxRepository.InflightRow row(long id, UUID orderId, String payload) {
        return new LedgerInflightOutboxRepository.InflightRow(id, orderId, payload, Instant.now(), 0);
    }

    private static final class NoopTransactionManager extends AbstractPlatformTransactionManager {
        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {}

        @Override
        protected void doCommit(DefaultTransactionStatus status) {}

        @Override
        protected void doRollback(DefaultTransactionStatus status) {}
    }
}
