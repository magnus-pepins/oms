package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.MarketContextRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import io.micrometer.core.instrument.MeterRegistry;
import org.agrona.ExpandableArrayBuffer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Poll-batch transaction boundary: one Postgres COMMIT per accumulated poll burst (up to
 * {@code maxFragmentsPerCommit} fragments), not per fragment or per single {@code replay.poll}.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorPollBatchTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long RECORDING_ID = 13L;

    private final OmsConfig config = new OmsConfig();
    @Mock private AeronProjectorCursorRepository cursorRepository;
    @Mock private OrdersRepository ordersRepository;
    @Mock private OrderControlAdmission controlAdmission;
    @Mock private ExecutionsRepository executionsRepository;
    @Mock private DomainEventOutboxRepository domainEventOutboxRepository;
    @Mock private LedgerInflightOutboxRepository ledgerInflightOutboxRepository;
    @Mock private DomainEventEnvelopeCodec envelopeCodec;
    @Mock private MarketContextRepository marketContextRepository;
    @Mock private PositionsRepository positionsRepository;
    @Mock private ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp;

    private final CountingTransactionManager txManager = new CountingTransactionManager();
    private MeterRegistry meterRegistry;
    private OmsPostgresProjector projector;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 5L), ZoneOffset.UTC);
        projector = new OmsPostgresProjector(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                ledgerInflightOutboxRepository,
                envelopeCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                new ObjectMapper(),
                txManager,
                pinned,
                new com.balh.oms.settlement.SettlementDateCalculator(
                        com.balh.oms.settlement.SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK),
                org.mockito.Mockito.mock(com.balh.oms.settlement.PredictionMarketResolutionService.class),
                null);
        projector.setCurrentRecordingIdForTesting(RECORDING_ID);
    }

    @Test
    void applyPollBatch_appliesThreeAdmitsInSingleTransaction() throws Exception {
        OrderAdmittedEvent ev1 = admitted("PREDMKT-1");
        OrderAdmittedEvent ev2 = admitted("PREDMKT-2");
        OrderAdmittedEvent ev3 = admitted("PREDMKT-3");
        when(ordersRepository.batchInsertFromAdmittedEvents(List.of(ev1, ev2, ev3))).thenReturn(new int[] {1, 1, 1});

        projector.applyPollBatchForTesting(List.of(
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev1, 100L),
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev2, 200L),
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev3, 300L)));

        assertThat(txManager.beginCount.get())
                .as("one BEGIN/COMMIT envelope for the whole poll batch")
                .isEqualTo(1);
        verify(ordersRepository).batchInsertFromAdmittedEvents(List.of(ev1, ev2, ev3));
        verify(ordersRepository, never())
                .insertFromAdmittedEventWithOrder(any(), nullable(OrdersRepository.PinnedFeeAtAdmit.class));
        verify(cursorRepository, times(1))
                .advanceWithRecording(
                        eq(OmsPostgresProjector.PROJECTOR_ID),
                        eq(OmsClusterWireFormat.EVENTS_STREAM_ID),
                        eq(RECORDING_ID),
                        eq(300L));
        assertThat(projector.lastAppliedPosition()).isEqualTo(300L);
    }

    @Test
    void shouldFlushPartialPollBatch_defersWhileCatchUpBacklog() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        projector.setLiveReplayUpperBoundForTesting(10_000L);
        projector.setLastAppliedPositionForTesting(0L);

        assertThat(projector.shouldFlushPartialPollBatch(projectorCfg, 512))
                .as("catch-up must not flush sub-cap batches on Archive micro-gaps")
                .isFalse();

        projector.setLiveReplayUpperBoundForTesting(0L);
        projector.setLastAppliedPositionForTesting(0L);
        assertThat(projector.shouldFlushPartialPollBatch(projectorCfg, 512)).isTrue();
        assertThat(projector.shouldFlushPartialPollBatch(projectorCfg, 0)).isFalse();
    }

    @Test
    void shouldFlushPartialPollBatch_defersWhileApplyQueueBackedUp() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        projector.setLastAppliedAcceptedAtMillisForTesting(ACCEPTED_AT_MS);
        ArrayBlockingQueue<OmsPostgresProjector.SequencedBatch> queue = new ArrayBlockingQueue<>(4);
        queue.offer(
                new OmsPostgresProjector.SequencedBatch(
                        1L,
                        List.of(new OmsPostgresProjector.PendingFragment.CursorOnly(64L))));
        projector.setApplyQueueForTesting(queue);

        assertThat(projector.shouldFlushPartialPollBatch(projectorCfg, 900))
                .as("non-empty apply queue must defer sub-cap partial flush at 16k soak")
                .isFalse();

        queue.clear();
        assertThat(projector.shouldFlushPartialPollBatch(projectorCfg, 900)).isTrue();
    }

    @Test
    void applyPollBatch_batchesLedgerInflightOutboxInserts() throws Exception {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);
        config.getLedger().setInflightPreAdmitHoldEnabled(false);

        OrderAdmittedEvent ev1 = admittedWithLedgerBalance("PREDMKT-L1", "bal-1");
        OrderAdmittedEvent ev2 = admittedWithLedgerBalance("PREDMKT-L2", "bal-2");
        OrderAdmittedEvent ev3 = admittedWithLedgerBalance("PREDMKT-L3", "bal-3");
        when(ordersRepository.batchInsertFromAdmittedEvents(List.of(ev1, ev2, ev3)))
                .thenReturn(new int[] {1, 1, 1});

        projector.applyPollBatchForTesting(List.of(
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev1, 100L),
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev2, 200L),
                new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev3, 300L)));

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
        verify(ledgerInflightOutboxRepository, times(1))
                .batchInsertIfAbsent(any(), any());
    }

    @Test
    void isAdmitOnlyPollBatch_trueForAdmits_falseForMixed() {
        OrderAdmittedEvent ev = admitted("PREDMKT-1");
        assertThat(
                        OmsPostgresProjector.isAdmitOnlyPollBatch(
                                List.of(new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev, 1L))))
                .isTrue();
        assertThat(
                        OmsPostgresProjector.isAdmitOnlyPollBatch(
                                List.of(
                                        new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev, 1L),
                                        new OmsPostgresProjector.PendingFragment.CursorOnly(2L))))
                .isFalse();
        assertThat(OmsPostgresProjector.isAdmitOnlyPollBatch(List.of())).isFalse();
    }

    @Test
    void applyPollBatch_recordsPollBatchCommitTimer() throws Exception {
        OrderAdmittedEvent ev = admitted("PREDMKT-1");
        stubFreshAdmitBatch(List.of(ev));

        projector.applyPollBatchForTesting(
                List.of(new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev, 512L)));

        Timer timer = meterRegistry.find(OmsPostgresProjector.TIMER_POLL_BATCH_COMMIT).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void applyPollBatch_emptyBatchIsNoOp() {
        projector.applyPollBatchForTesting(List.of());

        assertThat(txManager.beginCount.get()).isZero();
        verify(ordersRepository, never())
                .insertFromAdmittedEventWithOrder(any(), nullable(OrdersRepository.PinnedFeeAtAdmit.class));
        Timer timer = meterRegistry.find(OmsPostgresProjector.TIMER_POLL_BATCH_COMMIT).timer();
        assertThat(timer).isNull();
    }

    @Test
    void pollReplayBurst_tightSpinsUntilAeronReturnsZero() {
        projector.setRunningForTesting(true);
        config.getCluster().getProjector().setFragmentLimit(512);

        OmsPostgresProjector.ProjectingFragmentHandler handler =
                projector.createProjectingFragmentHandlerForTesting();
        Subscription replay = org.mockito.Mockito.mock(io.aeron.Subscription.class);
        AtomicInteger pollCalls = new AtomicInteger();
        when(replay.poll(any(), eq(4096)))
                .thenAnswer(
                        inv -> {
                            int n = pollCalls.getAndIncrement();
                            return switch (n) {
                                case 0 -> {
                                    deliverCursorOnlyFragment(handler, 64L);
                                    yield 2048;
                                }
                                case 1 -> {
                                    deliverCursorOnlyFragment(handler, 128L);
                                    yield 17;
                                }
                                default -> 0;
                            };
                        });

        int total =
                projector.pollReplayBurst(replay, handler, config.getCluster().getProjector());

        assertThat(total).isEqualTo(2065);
        assertThat(pollCalls.get()).isEqualTo(3);
        assertThat(txManager.beginCount.get())
                .as("partial batch defers flush until outer loop idle tail")
                .isZero();
        assertThat(handler.pendingFragmentCount()).isEqualTo(2);
    }

    @Test
    void pollReplayBurst_flushesWhenMaxFragmentsPerCommitReachedDuringProductivePoll() {
        projector.setRunningForTesting(true);
        config.getCluster().getProjector().setMaxFragmentsPerCommit(2);
        config.getCluster().getProjector().setFragmentLimit(1024);

        OmsPostgresProjector.ProjectingFragmentHandler handler =
                projector.createProjectingFragmentHandlerForTesting();
        Subscription replay = org.mockito.Mockito.mock(io.aeron.Subscription.class);
        AtomicInteger pollCalls = new AtomicInteger();
        when(replay.poll(any(), eq(4096)))
                .thenAnswer(
                        inv -> {
                            return switch (pollCalls.getAndIncrement()) {
                                case 0 -> {
                                    deliverCursorOnlyFragment(handler, 64L);
                                    deliverCursorOnlyFragment(handler, 128L);
                                    yield 2;
                                }
                                case 1 -> {
                                    deliverCursorOnlyFragment(handler, 192L);
                                    deliverCursorOnlyFragment(handler, 256L);
                                    yield 2;
                                }
                                default -> 0;
                            };
                        });

        int total =
                projector.pollReplayBurst(replay, handler, config.getCluster().getProjector());

        assertThat(total).isEqualTo(4);
        assertThat(txManager.beginCount.get())
                .as("each productive poll that hits the commit cap flushes before the next poll")
                .isEqualTo(2);
        assertThat(handler.pendingFragmentCount()).isZero();
    }

    @Test
    void effectiveFragmentLimit_floorsAt4096ForHighAdmitDrain() {
        assertThat(OmsPostgresProjector.effectiveFragmentLimit(512)).isEqualTo(4096);
        assertThat(OmsPostgresProjector.effectiveFragmentLimit(2048)).isEqualTo(4096);
        assertThat(OmsPostgresProjector.effectiveFragmentLimit(8192)).isEqualTo(8192);
    }

    @Test
    void effectiveMaxFragmentsPerCommit_usesCatchUpCapWhenReplayBacklogPresent() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        projectorCfg.setMaxFragmentsPerCommit(8192);
        projectorCfg.setCatchUpMaxFragmentsPerCommit(16_384);

        projector.setLiveReplayUpperBoundForTesting(10_000L);
        projector.setLastAppliedAcceptedAtMillisForTesting(ACCEPTED_AT_MS);
        assertThat(projector.lastAppliedPosition()).isZero();
        assertThat(projector.effectiveMaxFragmentsPerCommit(projectorCfg)).isEqualTo(16_384);

        projector.setLiveReplayUpperBoundForTesting(0L);
        assertThat(projector.effectiveMaxFragmentsPerCommit(projectorCfg)).isEqualTo(8192);
    }

    @Test
    void replayPollHasCatchUpBacklogByWallClock_retainedForBenchMetricsOnly() {
        assertThat(
                        OmsPostgresProjector.replayPollHasCatchUpBacklogByWallClock(
                                500L, ACCEPTED_AT_MS, ACCEPTED_AT_MS + 400L))
                .isFalse();
        assertThat(
                        OmsPostgresProjector.replayPollHasCatchUpBacklogByWallClock(
                                500L, ACCEPTED_AT_MS, ACCEPTED_AT_MS + 600L))
                .isTrue();
    }

    @Test
    void replayPollHasCatchUpBacklog_falseAtIdleHead_despiteOldAdmitWallClock() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        projector.setLastAppliedAcceptedAtMillisForTesting(ACCEPTED_AT_MS - 600_000L);
        projector.setLastAppliedPositionForTesting(4576L);
        projector.setLiveReplayUpperBoundForTesting(4576L);
        assertThat(projector.replayPollHasCatchUpBacklog(projectorCfg)).isFalse();
    }

    @Test
    void pollReplayIdleTail_retriesBurstBeforePartialFlush() {
        projector.setRunningForTesting(true);
        config.getCluster().getProjector().setFragmentLimit(1024);

        OmsPostgresProjector.ProjectingFragmentHandler handler =
                projector.createProjectingFragmentHandlerForTesting();
        deliverCursorOnlyFragment(handler, 64L);
        Subscription replay = org.mockito.Mockito.mock(io.aeron.Subscription.class);
        AtomicInteger pollCalls = new AtomicInteger();
        when(replay.poll(any(), eq(4096)))
                .thenAnswer(
                        inv -> {
                            if (pollCalls.getAndIncrement() == 2) {
                                deliverCursorOnlyFragment(handler, 128L);
                                return 1;
                            }
                            return 0;
                        });

        int idleTail =
                projector.pollReplayIdleTail(replay, handler, config.getCluster().getProjector());

        assertThat(idleTail).isEqualTo(1);
        assertThat(pollCalls.get()).isGreaterThanOrEqualTo(3);
        assertThat(txManager.beginCount.get()).isZero();
        assertThat(handler.pendingFragmentCount()).isEqualTo(2);
    }

    @Test
    void pollReplayIdleTail_runsDuringCatchUpBacklog() {
        projector.setRunningForTesting(true);
        projector.setLiveReplayUpperBoundForTesting(10_000L);
        projector.setLastAppliedPositionForTesting(0L);
        config.getCluster().getProjector().setFragmentLimit(512);

        OmsPostgresProjector.ProjectingFragmentHandler handler =
                projector.createProjectingFragmentHandlerForTesting();
        deliverCursorOnlyFragment(handler, 64L);
        Subscription replay = org.mockito.Mockito.mock(io.aeron.Subscription.class);
        AtomicInteger pollCalls = new AtomicInteger();
        when(replay.poll(any(), eq(4096)))
                .thenAnswer(
                        inv -> {
                            if (pollCalls.getAndIncrement() == 1) {
                                deliverCursorOnlyFragment(handler, 128L);
                                return 1;
                            }
                            return 0;
                        });

        int idleTail =
                projector.pollReplayIdleTail(replay, handler, config.getCluster().getProjector());

        assertThat(idleTail).isEqualTo(1);
        assertThat(pollCalls.get()).isGreaterThanOrEqualTo(2);
        assertThat(txManager.beginCount.get())
                .as("idle tail must accumulate before partial flush even when catch-up")
                .isZero();
        assertThat(handler.pendingFragmentCount()).isEqualTo(2);
    }

    @Test
    void applyPollBatch_benchFastPath_skipsPerEventWorkWhenAllSkipsActive() throws Exception {
        config.getLedger().setInflightPreAdmitHoldEnabled(true);
        projector.setSkipVenueControlPassAuditForTesting(true);
        projector.setSkipVenueOrderAcceptedOutboxForTesting(true);
        com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        try {
            OrderAdmittedEvent ev1 = admitted("PREDMKT-F1");
            OrderAdmittedEvent ev2 = admitted("PREDMKT-F2");
            when(ordersRepository.batchInsertFromAdmittedEvents(List.of(ev1, ev2)))
                    .thenReturn(new int[] {1, 1});

            projector.applyPollBatchForTesting(List.of(
                    new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev1, 100L),
                    new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev2, 200L)));

            assertThat(txManager.beginCount.get()).isEqualTo(1);
            verify(ordersRepository).batchInsertFromAdmittedEvents(List.of(ev1, ev2));
            verify(ordersRepository, never()).orderFromAdmittedEvent(any());
            verify(controlAdmission, never()).persistAdmission(any(), any(), Mockito.anyBoolean());
            verify(domainEventOutboxRepository, never()).insert(any(), any());
            verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
            verify(ledgerInflightOutboxRepository, never()).batchInsertIfAbsent(any(), any());
            assertThat(projector.lastAppliedPosition()).isEqualTo(200L);
        } finally {
            com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(null);
        }
    }

    @Test
    void isBenchAdmitOnlyFastPath_falseWhenAnyGateMissing() {
        config.getLedger().setInflightPreAdmitHoldEnabled(true);
        projector.setSkipVenueControlPassAuditForTesting(true);
        projector.setSkipVenueOrderAcceptedOutboxForTesting(true);
        com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        try {
            assertThat(projector.isBenchAdmitOnlyFastPath()).isTrue();

            projector.setSkipVenueOrderAcceptedOutboxForTesting(false);
            assertThat(projector.isBenchAdmitOnlyFastPath()).isFalse();

            projector.setSkipVenueOrderAcceptedOutboxForTesting(true);
            config.getLedger().setInflightPreAdmitHoldEnabled(false);
            config.getLedger().setInflightReservationEnabled(true);
            config.getLedger().setInflightAsyncEnabled(true);
            assertThat(projector.isBenchAdmitOnlyFastPath())
                    .as("ledger outbox pipeline active, can't fast-path")
                    .isFalse();
        } finally {
            com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(null);
        }
    }

    @Test
    void applyPollBatch_cachedEnvSkipsOrderConstruction_whenPassAndRiskSkipActive() throws Exception {
        projector.setSkipVenueControlPassAuditForTesting(true);
        com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        try {
            OrderAdmittedEvent ev1 = admitted("PREDMKT-X1");
            when(ordersRepository.batchInsertFromAdmittedEvents(List.of(ev1)))
                    .thenReturn(new int[] {1});

            projector.applyPollBatchForTesting(List.of(
                    new OmsPostgresProjector.PendingFragment.OrderAdmitted(ev1, 100L)));

            verify(ordersRepository, never()).orderFromAdmittedEvent(any());
            verify(controlAdmission, never()).persistAdmission(any(), any(), Mockito.anyBoolean());
        } finally {
            com.balh.oms.risk.ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(null);
        }
    }

    @Test
    void replayThreadPriority_clampedToThreadRange() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        projectorCfg.setReplayThreadPriority(Thread.MAX_PRIORITY + 10);
        assertThat(projectorCfg.getReplayThreadPriority()).isEqualTo(Thread.MAX_PRIORITY);
        projectorCfg.setReplayThreadPriority(Thread.MIN_PRIORITY - 10);
        assertThat(projectorCfg.getReplayThreadPriority()).isEqualTo(Thread.MIN_PRIORITY);
    }

    @Test
    void fragmentHandler_buffersUntilFlushThenCommitsOnce() throws Exception {
        OrderAdmittedEvent ev1 = admitted("PREDMKT-A");
        OrderAdmittedEvent ev2 = admitted("PREDMKT-B");
        stubFreshAdmitBatch(List.of(ev1, ev2));

        OmsPostgresProjector.ProjectingFragmentHandler handler =
                projector.createProjectingFragmentHandlerForTesting();
        deliverAdmittedFragment(handler, ev1, 64L);
        deliverAdmittedFragment(handler, ev2, 128L);

        assertThat(txManager.beginCount.get())
                .as("onFragment must not open a transaction")
                .isZero();

        handler.flushPollBatch();

        assertThat(txManager.beginCount.get()).isEqualTo(1);
        assertThat(projector.lastAppliedPosition()).isEqualTo(128L);
    }

    private static void deliverAdmittedFragment(
            OmsPostgresProjector.ProjectingFragmentHandler handler, OrderAdmittedEvent ev, long position) {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(1024);
        int length = ev.encode(buf, 0);
        Header header = Mockito.mock(Header.class);
        Mockito.when(header.position()).thenReturn(position);
        handler.onFragment(buf, 0, length, header);
    }

    private static void deliverCursorOnlyFragment(
            OmsPostgresProjector.ProjectingFragmentHandler handler, long position) {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(32);
        buf.putInt(OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, 99_999);
        Header header = Mockito.mock(Header.class);
        Mockito.when(header.position()).thenReturn(position);
        handler.onFragment(buf, 0, 32, header);
    }

    private void stubFreshAdmitBatch(List<OrderAdmittedEvent> events) throws Exception {
        int[] counts = new int[events.size()];
        java.util.Arrays.fill(counts, 1);
        when(ordersRepository.batchInsertFromAdmittedEvents(events)).thenReturn(counts);
        Order projected = org.mockito.Mockito.mock(Order.class);
        for (OrderAdmittedEvent ev : events) {
            when(ordersRepository.orderFromAdmittedEvent(ev)).thenReturn(projected);
            when(envelopeCodec.orderAcceptedFromAdmitted(ev)).thenReturn("{\"type\":\"OrderAccepted\"}");
        }
    }

    private static OrderAdmittedEvent admitted(String symbol) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                0L,
                ACCEPTED_AT_MS,
                10_000_000_000L,
                30_000L,
                0,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct",
                "idem-" + symbol,
                "hash",
                symbol,
                null);
    }

    private static OrderAdmittedEvent admittedWithLedgerBalance(String symbol, String balanceId) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                0L,
                ACCEPTED_AT_MS,
                10_000_000_000L,
                30_000L,
                0,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                UUID.randomUUID().toString(),
                "idem-" + symbol,
                "hash",
                symbol,
                balanceId);
    }

    private static final class CountingTransactionManager extends AbstractPlatformTransactionManager {
        final AtomicInteger beginCount = new AtomicInteger();

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            beginCount.incrementAndGet();
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
