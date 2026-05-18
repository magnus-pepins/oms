package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerInflightLifecycleClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository.LifecycleSettleableRow;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link LedgerInflightLifecycleReconciler}.
 *
 * <p>Stubs the {@link PlatformTransactionManager} so the reconciler's
 * {@link org.springframework.transaction.support.TransactionTemplate} executes the inline lambda
 * synchronously without standing up Postgres. Asserts:
 *
 * <ul>
 *   <li>FILLED orders → {@code commitHold} on the Ledger client + {@code markLifecycleSettled}
 *       with {@code "commit"};</li>
 *   <li>CANCELLED / REJECTED / EXPIRED orders → {@code voidHold} + {@code markLifecycleSettled}
 *       with {@code "void"};</li>
 *   <li>a per-row Ledger error → {@code markLifecycleFailed} and the counter increments; the
 *       loop keeps processing the remaining rows;</li>
 *   <li>config flags off (lifecycle reconciler disabled OR inflight-async disabled) →
 *       early-exit, no Postgres / Ledger interaction;</li>
 *   <li>missing Ledger client bean → early-exit.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
// LENIENT: half the tests in this class deliberately bail out of runOnce() before touching
// txManager / lifecycleClient (config-disabled, missing-bean). Stubbing those in @BeforeEach is
// the readable shape; switching to strict here would force per-test wiring duplication.
@MockitoSettings(strictness = Strictness.LENIENT)
class LedgerInflightLifecycleReconcilerTest {

    @Mock private LedgerInflightOutboxRepository outbox;
    @Mock private LedgerInflightLifecycleClient lifecycleClient;
    @Mock private PlatformTransactionManager txManager;
    @Mock private ObjectProvider<LedgerInflightLifecycleClient> lifecycleClientProvider;

    private OmsConfig config;
    private SimpleMeterRegistry meterRegistry;
    private LedgerInflightLifecycleReconciler reconciler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getLedger().setInflightAsyncEnabled(true);
        config.getLedger().setInflightLifecycleReconcilerEnabled(true);
        meterRegistry = new SimpleMeterRegistry();
        // TransactionTemplate calls TransactionManager.getTransaction() then commit/rollback —
        // SimpleTransactionStatus is the cheapest stand-in that lets the lambda run inline.
        when(txManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        when(lifecycleClientProvider.getIfAvailable()).thenReturn(lifecycleClient);
        reconciler = new LedgerInflightLifecycleReconciler(
                outbox, lifecycleClientProvider, config, meterRegistry, txManager);
    }

    @Test
    void filledOrder_commitsHold_andMarksSettledWithCommit() throws Exception {
        UUID orderId = UUID.randomUUID();
        when(outbox.fetchLifecycleSettleable(anyInt(), anyInt(), any()))
                .thenReturn(List.of(new LifecycleSettleableRow(
                        /* id = */ 7L, orderId, "txn_abc", 0, "FILLED", 0)));

        reconciler.runOnce();

        verify(lifecycleClient).commitHold("txn_abc");
        verify(outbox).markLifecycleSettled(eq(7L), eq("commit"), any(Instant.class));
        verify(outbox, never()).markLifecycleFailed(anyLong(), anyString(), any(Instant.class));
        assertThat(meterRegistry.find("oms_ledger_inflight_lifecycle_settled_total")
                .tag("action", "commit").counter().count()).isEqualTo(1.0);
    }

    @Test
    void cancelledOrder_voidsHold_andMarksSettledWithVoid() throws Exception {
        UUID orderId = UUID.randomUUID();
        when(outbox.fetchLifecycleSettleable(anyInt(), anyInt(), any()))
                .thenReturn(List.of(new LifecycleSettleableRow(
                        8L, orderId, "txn_def", 0, "CANCELLED", 0)));

        reconciler.runOnce();

        verify(lifecycleClient).voidHold("txn_def");
        verify(outbox).markLifecycleSettled(eq(8L), eq("void"), any(Instant.class));
        assertThat(meterRegistry.find("oms_ledger_inflight_lifecycle_settled_total")
                .tag("action", "void").counter().count()).isEqualTo(1.0);
    }

    @Test
    void rejectedAndExpired_alsoVoidHold() throws Exception {
        when(outbox.fetchLifecycleSettleable(anyInt(), anyInt(), any()))
                .thenReturn(List.of(
                        new LifecycleSettleableRow(10L, UUID.randomUUID(), "txn_1", 0, "REJECTED", 0),
                        new LifecycleSettleableRow(11L, UUID.randomUUID(), "txn_2", 0, "EXPIRED", 0)));

        reconciler.runOnce();

        verify(lifecycleClient).voidHold("txn_1");
        verify(lifecycleClient).voidHold("txn_2");
        verify(outbox).markLifecycleSettled(eq(10L), eq("void"), any());
        verify(outbox).markLifecycleSettled(eq(11L), eq("void"), any());
        assertThat(meterRegistry.find("oms_ledger_inflight_lifecycle_settled_total")
                .tag("action", "void").counter().count()).isEqualTo(2.0);
    }

    @Test
    void ledgerError_marksFailed_doesNotMarkSettled_continuesLoop() throws Exception {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        when(outbox.fetchLifecycleSettleable(anyInt(), anyInt(), any()))
                .thenReturn(List.of(
                        new LifecycleSettleableRow(20L, first, "txn_fail", 0, "FILLED", 0),
                        new LifecycleSettleableRow(21L, second, "txn_ok", 0, "FILLED", 0)));
        doThrow(new LedgerInflightLifecycleClient.LedgerLifecycleException("503 down"))
                .when(lifecycleClient).commitHold("txn_fail");

        reconciler.runOnce();

        ArgumentCaptor<String> errCaptor = ArgumentCaptor.forClass(String.class);
        verify(outbox).markLifecycleFailed(eq(20L), errCaptor.capture(), any(Instant.class));
        assertThat(errCaptor.getValue()).contains("503 down");
        verify(outbox, never()).markLifecycleSettled(eq(20L), anyString(), any());

        verify(lifecycleClient).commitHold("txn_ok");
        verify(outbox).markLifecycleSettled(eq(21L), eq("commit"), any());

        assertThat(meterRegistry.find("oms_ledger_inflight_lifecycle_failed_total")
                .tag("action", "commit").counter().count()).isEqualTo(1.0);
        assertThat(meterRegistry.find("oms_ledger_inflight_lifecycle_settled_total")
                .tag("action", "commit").counter().count()).isEqualTo(1.0);
    }

    @Test
    void disabled_lifecycleReconcilerFlag_earlyExit() {
        config.getLedger().setInflightLifecycleReconcilerEnabled(false);

        reconciler.runOnce();

        verifyNoInteractions(outbox);
        verifyNoInteractions(lifecycleClient);
    }

    @Test
    void disabled_inflightAsyncFlag_earlyExit() {
        config.getLedger().setInflightAsyncEnabled(false);

        reconciler.runOnce();

        verifyNoInteractions(outbox);
        verifyNoInteractions(lifecycleClient);
    }

    @Test
    void missingLifecycleClient_earlyExit() {
        when(lifecycleClientProvider.getIfAvailable()).thenReturn(null);

        reconciler.runOnce();

        verifyNoInteractions(outbox);
    }

    @Test
    void actionForStatus_mapsAllKnownTerminalsAndReturnsNullForOthers() {
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("FILLED")).isEqualTo("commit");
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("CANCELLED")).isEqualTo("void");
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("REJECTED")).isEqualTo("void");
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("EXPIRED")).isEqualTo("void");
        // Non-terminal / unexpected — reconciler skips with a warn log; null gates that branch.
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("WORKING")).isNull();
        assertThat(LedgerInflightLifecycleReconciler.actionForStatus("PARTIALLY_FILLED")).isNull();
    }

    private static long anyLong() {
        return org.mockito.ArgumentMatchers.anyLong();
    }
}
