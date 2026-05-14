package com.balh.oms.reconciler;

import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LedgerInflightHoldFailureCompensator} (slice 4p).
 *
 * <p>Covers the failure-mode invariants the plan calls out:
 * <ul>
 *   <li>Disabled by config: zero side effects.</li>
 *   <li>Cluster client missing or disconnected: zero side effects.</li>
 *   <li>Successful cancel: row is marked compensated with the same correlation id used on the
 *       cluster offer (ops-debugging linkage).</li>
 *   <li>Cluster submit timeout: the batch is aborted (do not burn a 2 s budget per row when the
 *       cluster is unreachable).</li>
 *   <li>Reason truncation: a long {@code last_error} is truncated below the wire-format string
 *       cap so the encoded {@link CancelOrderCommand} cannot blow up at the codec boundary.</li>
 * </ul>
 */
class LedgerInflightHoldFailureCompensatorTest {

    private LedgerInflightOutboxRepository outbox;
    private OmsClusterIngressClient client;
    private ObjectProvider<OmsClusterShardRouter> clusterShardRouter;
    private OmsConfig config;
    private SimpleMeterRegistry meterRegistry;
    private LedgerInflightHoldFailureCompensator compensator;
    private AtomicLong correlationCounter;

    @BeforeEach
    void setUp() {
        outbox = mock(LedgerInflightOutboxRepository.class);
        client = mock(OmsClusterIngressClient.class);
        when(client.isConnected()).thenReturn(true);
        correlationCounter = new AtomicLong(1000L);
        when(client.nextCorrelationId()).thenAnswer(inv -> correlationCounter.getAndIncrement());
        // Phase 4 Tier 2.5 phase E-2: the compensator now routes via OmsClusterShardRouter
        // (E-1 introduced it). At shardCount=1 the router wraps a single client — same instance
        // every test method here exercises, so all pre-E-2 assertions on `client` still hold.
        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, client));
        @SuppressWarnings("unchecked")
        ObjectProvider<OmsClusterShardRouter> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(router);
        clusterShardRouter = provider;
        config = newEnabledConfig();
        meterRegistry = new SimpleMeterRegistry();
        compensator = new LedgerInflightHoldFailureCompensator(
                outbox, clusterShardRouter, config, meterRegistry, new NoopTransactionManager());
    }

    @Test
    void runOnce_disabled_isCheapNoOp() {
        config.getLedger().setInflightCompensatorEnabled(false);

        compensator.runOnce();

        verify(outbox, never()).fetchFailedUncompensated(anyInt(), anyInt());
    }

    @Test
    void runOnce_clusterClientDisconnected_skipsRowsAndCountsSubmitFailed() {
        // Phase 4 Tier 2.5 phase E-2: connect-check moved per-row (the router can host clients
        // for several shards; one shard's client being down should not block another shard's
        // rows). For the single-shard test config that means: outbox is queried, but each row is
        // skipped without a cluster offer, and each skip increments the submit_failed counter.
        when(client.isConnected()).thenReturn(false);
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-00000000B001");
        var row = new LedgerInflightOutboxRepository.FailedInflightRow(
                73L, orderId, /* attempts = */ 3, "x", /* shardId = */ 0);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt())).thenReturn(List.of(row));

        compensator.runOnce();

        verify(outbox).fetchFailedUncompensated(anyInt(), anyInt());
        verify(outbox, never()).markCompensated(anyLong(), anyLong(), any());
        assertThat(meterRegistry.counter("oms_ledger_inflight_hold_compensate_failed_total",
                "outcome", "submit_failed").count()).isEqualTo(1.0);
    }

    @Test
    void runOnce_emptyBatch_doesNotSubmitOrMark() {
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt())).thenReturn(List.of());

        compensator.runOnce();

        verify(outbox, never()).markCompensated(anyLong(), anyLong(), any());
    }

    @Test
    void runOnce_successfulCancel_marksCompensatedWithSameCorrelationId() throws Exception {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-00000000A001");
        var row = new LedgerInflightOutboxRepository.FailedInflightRow(
                42L, orderId, /* attempts = */ 3, "insufficient_balance", /* shardId = */ 0);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt())).thenReturn(List.of(row));

        compensator.runOnce();

        ArgumentCaptor<CancelOrderCommand> cmdCaptor = ArgumentCaptor.forClass(CancelOrderCommand.class);
        verify(client).submitCancelOrder(cmdCaptor.capture(), any(Duration.class));
        long usedCorrelation = cmdCaptor.getValue().correlationId();
        assertThat(cmdCaptor.getValue().orderId()).isEqualTo(orderId);
        assertThat(cmdCaptor.getValue().reason()).contains("ledger_inflight_hold_failed:insufficient_balance");

        verify(outbox).markCompensated(eq(42L), eq(usedCorrelation), any());
        assertThat(meterRegistry.counter("oms_ledger_inflight_hold_compensated_total",
                "outcome", "cancelled").count()).isEqualTo(1.0);
    }

    @Test
    void runOnce_clusterTimeout_abortsBatch() throws Exception {
        UUID first = UUID.fromString("00000000-0000-4000-8000-00000000A002");
        UUID second = UUID.fromString("00000000-0000-4000-8000-00000000A003");
        var rowA = new LedgerInflightOutboxRepository.FailedInflightRow(101L, first, 3, "balance", 0);
        var rowB = new LedgerInflightOutboxRepository.FailedInflightRow(102L, second, 3, "balance", 0);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt())).thenReturn(List.of(rowA, rowB));
        doThrow(new TimeoutException("cluster offer back-pressure"))
                .when(client).submitCancelOrder(any(), any());

        compensator.runOnce();

        // Row A failed -> batch aborts; row B is never attempted, never marked.
        verify(client, times(1)).submitCancelOrder(any(), any());
        verify(outbox, never()).markCompensated(anyLong(), anyLong(), any());
        assertThat(meterRegistry.counter("oms_ledger_inflight_hold_compensate_failed_total",
                "outcome", "submit_failed").count()).isEqualTo(1.0);
    }

    @Test
    void runOnce_perRowRuntimeFailure_continuesToNextRow() throws Exception {
        UUID first = UUID.fromString("00000000-0000-4000-8000-00000000A004");
        UUID second = UUID.fromString("00000000-0000-4000-8000-00000000A005");
        var rowA = new LedgerInflightOutboxRepository.FailedInflightRow(201L, first, 3, "x", 0);
        var rowB = new LedgerInflightOutboxRepository.FailedInflightRow(202L, second, 3, "y", 0);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt())).thenReturn(List.of(rowA, rowB));
        // First call throws a runtime; second succeeds. The compensator must process row B.
        doThrow(new RuntimeException("aeron"))
                .doNothing()
                .when(client).submitCancelOrder(any(), any());

        compensator.runOnce();

        verify(client, times(2)).submitCancelOrder(any(), any());
        verify(outbox, times(1)).markCompensated(eq(202L), anyLong(), any());
        verify(outbox, never()).markCompensated(eq(201L), anyLong(), any());
    }

    @Test
    void runOnce_multiShard_dispatchesEachRowToItsOwningShard() throws Exception {
        // Phase 4 Tier 2.5 phase E-2 — pin the routing contract: at shardCount=2 a batch with
        // rows from both shards must dispatch each row's submitCancelOrder on the correct
        // client, and one shard being unconnected must not block the other shard's rows.
        OmsClusterIngressClient s0 = mock(OmsClusterIngressClient.class);
        OmsClusterIngressClient s1 = mock(OmsClusterIngressClient.class);
        when(s0.isConnected()).thenReturn(true);
        when(s1.isConnected()).thenReturn(true);
        AtomicLong corrSeq = new AtomicLong(2_000L);
        when(s0.nextCorrelationId()).thenAnswer(inv -> corrSeq.getAndIncrement());
        when(s1.nextCorrelationId()).thenAnswer(inv -> corrSeq.getAndIncrement());

        OmsClusterShardRouter router = new OmsClusterShardRouter(2, Map.of(0, s0, 1, s1));
        @SuppressWarnings("unchecked")
        ObjectProvider<OmsClusterShardRouter> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(router);

        LedgerInflightHoldFailureCompensator multiShardCompensator =
                new LedgerInflightHoldFailureCompensator(
                        outbox, provider, config, meterRegistry, new NoopTransactionManager());

        UUID orderS0 = UUID.fromString("00000000-0000-4000-8000-00000000C001");
        UUID orderS1 = UUID.fromString("00000000-0000-4000-8000-00000000C002");
        var rowS0 = new LedgerInflightOutboxRepository.FailedInflightRow(
                301L, orderS0, 3, "balance", /* shardId = */ 0);
        var rowS1 = new LedgerInflightOutboxRepository.FailedInflightRow(
                302L, orderS1, 3, "balance", /* shardId = */ 1);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt()))
                .thenReturn(List.of(rowS0, rowS1));

        multiShardCompensator.runOnce();

        // Shard 0's row must hit s0; shard 1's must hit s1; never the other way.
        verify(s0, times(1)).submitCancelOrder(any(), any());
        verify(s1, times(1)).submitCancelOrder(any(), any());
        verify(outbox, times(1)).markCompensated(eq(301L), anyLong(), any());
        verify(outbox, times(1)).markCompensated(eq(302L), anyLong(), any());
    }

    @Test
    void runOnce_multiShard_oneShardDownDoesNotBlockOtherShard() throws Exception {
        OmsClusterIngressClient s0 = mock(OmsClusterIngressClient.class);
        OmsClusterIngressClient s1 = mock(OmsClusterIngressClient.class);
        when(s0.isConnected()).thenReturn(false); // shard 0 down
        when(s1.isConnected()).thenReturn(true);
        AtomicLong corrSeq = new AtomicLong(3_000L);
        when(s1.nextCorrelationId()).thenAnswer(inv -> corrSeq.getAndIncrement());

        OmsClusterShardRouter router = new OmsClusterShardRouter(2, Map.of(0, s0, 1, s1));
        @SuppressWarnings("unchecked")
        ObjectProvider<OmsClusterShardRouter> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(router);

        LedgerInflightHoldFailureCompensator multiShardCompensator =
                new LedgerInflightHoldFailureCompensator(
                        outbox, provider, config, meterRegistry, new NoopTransactionManager());

        var rowS0 = new LedgerInflightOutboxRepository.FailedInflightRow(
                401L, UUID.fromString("00000000-0000-4000-8000-00000000D001"), 3, "x", 0);
        var rowS1 = new LedgerInflightOutboxRepository.FailedInflightRow(
                402L, UUID.fromString("00000000-0000-4000-8000-00000000D002"), 3, "y", 1);
        when(outbox.fetchFailedUncompensated(anyInt(), anyInt()))
                .thenReturn(List.of(rowS0, rowS1));

        multiShardCompensator.runOnce();

        // Shard 0 row deferred (counted as submit_failed); shard 1 row succeeds.
        verify(s0, never()).submitCancelOrder(any(), any());
        verify(s1, times(1)).submitCancelOrder(any(), any());
        verify(outbox, times(1)).markCompensated(eq(402L), anyLong(), any());
        verify(outbox, never()).markCompensated(eq(401L), anyLong(), any());
        assertThat(meterRegistry.counter("oms_ledger_inflight_hold_compensate_failed_total",
                "outcome", "submit_failed").count()).isEqualTo(1.0);
    }

    @Test
    void buildReason_truncatesPastBudget_keepsPrefix() {
        String huge = "x".repeat(500);
        String reason = LedgerInflightHoldFailureCompensator.buildReason(huge);
        // Wire-format cap is 256 bytes; the compensator ceiling stays at 240 to leave headroom.
        assertThat(reason.length()).isLessThanOrEqualTo(240);
        assertThat(reason).startsWith("ledger_inflight_hold_failed:");
    }

    @Test
    void buildReason_nullLastError_returnsPrefixOnly() {
        assertThat(LedgerInflightHoldFailureCompensator.buildReason(null))
                .isEqualTo("ledger_inflight_hold_failed:");
    }

    private static OmsConfig newEnabledConfig() {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        cfg.getLedger().setInflightReservationEnabled(true);
        cfg.getLedger().setInflightAsyncEnabled(true);
        cfg.getLedger().setInflightCompensatorEnabled(true);
        cfg.getLedger().setInflightCompensatorAttemptsThreshold(3);
        cfg.getLedger().setInflightCompensatorBatchSize(50);
        return cfg;
    }

    /**
     * Spring's TransactionTemplate insists on a real {@link PlatformTransactionManager}; this stub
     * runs the lambda directly without any transaction-management semantics. Test-only; unit
     * coverage of the SQL/transaction wiring lives in IT, not here.
     */
    private static final class NoopTransactionManager extends AbstractPlatformTransactionManager {
        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            // no-op
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
            // no-op
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
            // no-op
        }
    }
}
