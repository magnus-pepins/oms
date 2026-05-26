package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelAppliedEvent;
import com.balh.oms.config.OmsConfig;
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Phase 4 Tier 2.5 phase E-2 — defensive shard guard on
 * {@link OmsPostgresProjector#applyAdmittedEvent}. At {@code shardCount=1} every event carries
 * {@code shardId=0} and {@link OmsConfig.Shard#getId()} defaults to {@code 0}, so the guard is
 * a no-op (verified by the existing happy-path tests). These tests pin the multi-shard
 * mismatch contract: an event whose {@code shardId} differs from this projector's
 * {@code OmsConfig.Shard.id} must be dropped without touching Postgres, the
 * {@link OmsPostgresProjector#METRIC_SHARD_MISMATCH_DROPPED} counter must increment, and the
 * cursor must still advance so the projector does not loop on the misrouted event.
 *
 * <p>Mock setup mirrors {@link OmsPostgresProjectorAdmitTimerTest} (the existing happy-path
 * test of the same method) deliberately — the projector wires through a lot of collaborators
 * but the guard short-circuits before any of them run, so most are never invoked here.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorShardGuardTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long FRAGMENT_POSITION = 8192L;

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
    @Mock private PlatformTransactionManager txManager;

    private MeterRegistry meterRegistry;
    private OmsPostgresProjector projector;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS), ZoneOffset.UTC);
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
                org.mockito.Mockito.mock(com.balh.oms.settlement.PredictionMarketResolutionService.class));
        // Production seeding (init() → bootstrap → replay loop) is bypassed in unit tests that
        // drive applyAdmittedEvent / applyOrderCancelAppliedEvent directly. Seed the recording
        // id so the apply path's cursor write does not fail loud on -1.
        projector.setCurrentRecordingIdForTesting(13L);
    }

    @Test
    void applyAdmittedEvent_matchingShard_appliesNormally() {
        // Pin the byte-identical-at-N=1 invariant explicitly: with the projector at shard 0 (the
        // default) and an event carrying shardId=0, the guard does not fire and the orders
        // insert is invoked. This must hold so existing single-shard production behaviour is
        // unchanged by the E-2 guard.
        config.getShard().setId(0);
        OrderAdmittedEvent ev = sampleAdmitted(/* shardId = */ 0);
        when(ordersRepository.insertFromAdmittedEvent(ev)).thenReturn(true);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ordersRepository).insertFromAdmittedEvent(ev);
        verify(cursorRepository).advanceWithRecording(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                13L,
                FRAGMENT_POSITION);
        assertThat(meterRegistry.counter(OmsPostgresProjector.METRIC_SHARD_MISMATCH_DROPPED)
                .count()).isEqualTo(0.0);
    }

    @Test
    void applyAdmittedEvent_mismatchedShard_dropsWithoutPostgresIO_advancesCursor_increments() {
        // Projector is at shard 1 but an event for shard 0 arrives — exactly the config-bug
        // shape this guard exists to catch. Postgres I/O must not happen; the cursor must
        // still advance (otherwise the projector loops forever on the same misrouted event);
        // the dropped-counter must increment by one.
        config.getShard().setId(1);
        OrderAdmittedEvent ev = sampleAdmitted(/* shardId = */ 0);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ordersRepository, never()).insertFromAdmittedEvent(any());
        verify(controlAdmission, never()).persistAdmission(any());
        verify(domainEventOutboxRepository, never()).insert(any(), any());
        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
        verify(cursorRepository).advanceWithRecording(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                13L,
                FRAGMENT_POSITION);
        assertThat(meterRegistry.counter(OmsPostgresProjector.METRIC_SHARD_MISMATCH_DROPPED)
                .count()).isEqualTo(1.0);
    }

    @Test
    void applyAdmittedEvent_repeatedMismatch_countsEach() {
        config.getShard().setId(0);
        OrderAdmittedEvent first = sampleAdmitted(/* shardId = */ 1);
        OrderAdmittedEvent second = sampleAdmitted(/* shardId = */ 1);
        OrderAdmittedEvent third = sampleAdmitted(/* shardId = */ 2);

        projector.applyAdmittedEvent(first, FRAGMENT_POSITION);
        projector.applyAdmittedEvent(second, FRAGMENT_POSITION + 1L);
        projector.applyAdmittedEvent(third, FRAGMENT_POSITION + 2L);

        verify(ordersRepository, never()).insertFromAdmittedEvent(any());
        assertThat(meterRegistry.counter(OmsPostgresProjector.METRIC_SHARD_MISMATCH_DROPPED)
                .count()).isEqualTo(3.0);
    }

    @Test
    void applyOrderCancelAppliedEvent_mismatchedShard_dropsWithoutPostgresIO_advancesCursor_increments() {
        // Phase 4 Tier 2.5 phase E-3b: the cluster service now propagates the order's actual
        // shardId onto OrderCancelAppliedEvent (previously hardcoded to 0). The projector's
        // shard guard now applies on the cancel branch too — same drop + counter + cursor
        // advance contract as the admit branch.
        config.getShard().setId(1);
        OrderCancelAppliedEvent ev = sampleCancel(/* shardId = */ 0);

        projector.applyOrderCancelAppliedEvent(ev, FRAGMENT_POSITION);

        verify(ordersRepository, never()).findById(any());
        verify(cursorRepository).advanceWithRecording(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                13L,
                FRAGMENT_POSITION);
        assertThat(meterRegistry.counter(OmsPostgresProjector.METRIC_SHARD_MISMATCH_DROPPED)
                .count()).isEqualTo(1.0);
    }

    private static OrderCancelAppliedEvent sampleCancel(int shardId) {
        return new OrderCancelAppliedEvent(
                UUID.randomUUID(),
                /* cancelledAtMillis = */ ACCEPTED_AT_MS + 1L,
                /* newVersion = */ 1,
                shardId,
                "acct",
                "hash",
                "AAPL",
                "ledger_inflight_hold_failed:test");
    }

    private static OrderAdmittedEvent sampleAdmitted(int shardId) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 1L,
                /* limitPriceScaledOrZero = */ 0L,
                shardId,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_GTC,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }
}
