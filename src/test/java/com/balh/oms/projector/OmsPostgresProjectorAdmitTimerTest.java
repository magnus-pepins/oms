package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
import com.balh.oms.observability.metrics.OmsPipelineMeterNames;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Unit-level coverage for the Phase 4j cluster-admit-to-projector Timer recorded at the bottom of
 * {@link OmsPostgresProjector#applyAdmittedEvent}. The full DB / replay / Aeron path is covered by
 * {@code OmsPostgresProjectorIT}; this test pins the wall-clock and stubs every collaborator so we
 * can assert the Timer's recorded duration deterministically.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorAdmitTimerTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long NOW_MS = ACCEPTED_AT_MS + 23L;
    private static final long FRAGMENT_POSITION = 8192L;

    // Real OmsConfig (defaults: ledger inflight reservation/async/coalescer all false) so the
    // D-1 backfill in applyAdmittedEvent early-returns and this test focuses on the timer
    // contract. Mocking OmsConfig + stubbing every nested getter would only fight the Mockito
    // strict-stubbing surface for no behavioural gain here.
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
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
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
        // Seed the recording id for the apply path's cursor write (unit tests bypass the replay loop).
        projector.setCurrentRecordingIdForTesting(13L);
    }

    @Test
    void applyAdmittedEvent_recordsAdmitToProjectorTimer() {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_GTC);
        Order projected = org.mockito.Mockito.mock(Order.class);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(true, projected));

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ordersRepository).insertFromAdmittedEventWithOrder(ev);
        verify(ordersRepository, never()).orderFromAdmittedEvent(any());
        verify(controlAdmission).persistAdmission(any(), eq(projected), eq(false));
        verify(cursorRepository).advanceWithRecording(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                13L,
                FRAGMENT_POSITION);

        Timer timer = meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_PROJECTOR)
                .tags(Tags.of(
                        OmsPipelineMetrics.TAG_PROJECTOR_ID, OmsPostgresProjector.PROJECTOR_ID,
                        OmsPipelineMetrics.TAG_SIDE, "buy",
                        OmsPipelineMetrics.TAG_TIF, "gtc"))
                .timer();
        assertThat(timer)
                .as("admit-to-projector Timer registered with expected (projector, side, tif) tags")
                .isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .isEqualTo((double) (NOW_MS - ACCEPTED_AT_MS));
    }

    @Test
    void applyAdmittedEvent_negativeLatencyClampedToZero() {
        long acceptedInTheFuture = NOW_MS + 9_000L;
        OrderAdmittedEvent ev = new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ acceptedInTheFuture,
                /* quantityScaled = */ 1L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_FOK,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(
                        true, org.mockito.Mockito.mock(Order.class)));

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        Timer timer = meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_PROJECTOR)
                .tags(Tags.of(
                        OmsPipelineMetrics.TAG_PROJECTOR_ID, OmsPostgresProjector.PROJECTOR_ID,
                        OmsPipelineMetrics.TAG_SIDE, "sell",
                        OmsPipelineMetrics.TAG_TIF, "fok"))
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .as("Math.max(0, ...) clamp keeps NTP-slew samples at 0 ms")
                .isEqualTo(0.0);
    }

    @Test
    void replay_insertReturnsFalse_skipsControlAdmission() {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(
                        false, org.mockito.Mockito.mock(Order.class)));

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(controlAdmission, never()).persistAdmission(any());
        verify(controlAdmission, never()).persistAdmission(any(), any());
        verify(controlAdmission, never()).persistAdmission(any(), any(), any(Boolean.class));
    }

    @Test
    void applyAdmittedEvent_failedInsert_doesNotRecordTimer() {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY);
        Mockito.doThrow(new RuntimeException("simulated SQL failure"))
                .when(ordersRepository).insertFromAdmittedEventWithOrder(ev);

        try {
            projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);
        } catch (RuntimeException expected) {
            // Bubbles up so the wrapping TransactionTemplate rolls back the transaction.
        }

        assertThat(meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_PROJECTOR).timer())
                .as("a SQL failure rolls back the transaction; the Timer must not register a sample"
                        + " for the failed event")
                .isNull();
    }

    private static OrderAdmittedEvent sampleAdmitted(byte side, byte tif) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 1L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                /* version = */ 0,
                side,
                tif,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }
}
