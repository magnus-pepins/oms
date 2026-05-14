package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderCancelAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.MarketContextRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

/**
 * Unit coverage for the slice 4p
 * {@link OmsPostgresProjector#applyOrderCancelAppliedEvent OrderCancelAppliedEvent} projector
 * branch. Pins the contract that distinguishes the OMS-initiated cancel from the venue-side
 * {@code EXEC_TYPE_CANCEL} branch:
 * <ul>
 *   <li>No {@code executions} insert (the cancel never touched a venue).</li>
 *   <li>{@code orders} CAS to CANCELLED with empty {@code venueId}/{@code venueExecRef} on the
 *       domain envelope.</li>
 *   <li>Replay over an already-CANCELLED row writes no new envelope (no double fanout).</li>
 *   <li>Race: a concurrent fill that already moved the row to FILLED leaves the terminal state
 *       untouched and skips envelope emission.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorOrderCancelAppliedTest {

    private static final long CANCELLED_AT_MS = 1_700_000_000_000L;
    private static final long FRAGMENT_POSITION = 8192L;

    @Mock private OmsConfig config;
    @Mock private AeronProjectorCursorRepository cursorRepository;
    @Mock private OrdersRepository ordersRepository;
    @Mock private OrderControlAdmission controlAdmission;
    @Mock private ExecutionsRepository executionsRepository;
    @Mock private DomainEventOutboxRepository domainEventOutboxRepository;
    @Mock private DomainEventEnvelopeCodec envelopeCodec;
    @Mock private MarketContextRepository marketContextRepository;
    @Mock private PositionsRepository positionsRepository;
    @Mock private ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp;
    @Mock private PlatformTransactionManager txManager;

    private SimpleMeterRegistry meterRegistry;
    private OmsPostgresProjector projector;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(CANCELLED_AT_MS + 5L), ZoneOffset.UTC);
        projector = new OmsPostgresProjector(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                envelopeCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                new ObjectMapper(),
                txManager,
                pinned);
    }

    @Test
    void applyOrderCancelApplied_workingOrder_writesCAS_andEmptyVenueEnvelope_noExecutionsInsert() throws Exception {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005001");
        Order working = orderAt(orderId, /* version = */ 1, OrderStatus.WORKING);
        Order refreshed = orderAt(orderId, /* version = */ 2, OrderStatus.CANCELLED);
        when(ordersRepository.findById(orderId))
                .thenReturn(Optional.of(working))
                .thenReturn(Optional.of(refreshed));
        when(ordersRepository.updateFillOrCancelWithCas(
                eq(orderId), eq(1), any(BigDecimal.class), eq(OrderStatus.CANCELLED), eq(null), any()))
                .thenReturn(true);
        when(envelopeCodec.orderCancelled(any(Order.class), anyInt(), anyString(), anyString()))
                .thenReturn("{\"type\":\"OrderCancelled\"}");

        OrderCancelAppliedEvent ev = sampleEvent(orderId, /* newVersion = */ 2);

        projector.applyOrderCancelAppliedEvent(ev, FRAGMENT_POSITION);

        verify(executionsRepository, never()).tryInsertCancel(any(), any(), any(), any(), any(), any(), any());
        verify(envelopeCodec).orderCancelled(any(Order.class), eq(2), eq(""), eq(""));
        verify(domainEventOutboxRepository).insert(eq(orderId), eq("{\"type\":\"OrderCancelled\"}"));
        verify(cursorRepository).advance(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                FRAGMENT_POSITION);
    }

    @Test
    void applyOrderCancelApplied_replayOnAlreadyCancelledRow_advancesCursorOnly() throws Exception {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005002");
        Order alreadyCancelled = orderAt(orderId, 2, OrderStatus.CANCELLED);
        when(ordersRepository.findById(orderId)).thenReturn(Optional.of(alreadyCancelled));

        projector.applyOrderCancelAppliedEvent(sampleEvent(orderId, 2), FRAGMENT_POSITION);

        verify(ordersRepository, never()).updateFillOrCancelWithCas(
                any(), anyInt(), any(), any(), any(), any());
        verify(envelopeCodec, never()).orderCancelled(any(), anyInt(), anyString(), anyString());
        verify(domainEventOutboxRepository, never()).insert(any(), anyString());
        verify(cursorRepository, times(1)).advance(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                FRAGMENT_POSITION);
    }

    @Test
    void applyOrderCancelApplied_casMissesBecauseOrderAlreadyFilled_noEnvelopeEmitted() throws Exception {
        // Documented race: the venue fill committed first, the projector saw FILLED and CASd to
        // version=2 with FILLED. Now this OrderCancelApplied arrives — CAS misses (we expect
        // version=1 but it's at 2), and the projector silently advances the cursor without
        // emitting an OrderCancelled envelope (we never want to fanout cancel-of-filled).
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005003");
        Order workingForRead = orderAt(orderId, 1, OrderStatus.WORKING);
        when(ordersRepository.findById(orderId)).thenReturn(Optional.of(workingForRead));
        when(ordersRepository.updateFillOrCancelWithCas(
                eq(orderId), eq(1), any(), eq(OrderStatus.CANCELLED), eq(null), any()))
                .thenReturn(false);

        projector.applyOrderCancelAppliedEvent(sampleEvent(orderId, 2), FRAGMENT_POSITION);

        verify(envelopeCodec, never()).orderCancelled(any(), anyInt(), anyString(), anyString());
        verify(domainEventOutboxRepository, never()).insert(any(), anyString());
        verify(cursorRepository).advance(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                FRAGMENT_POSITION);
    }

    @Test
    void applyOrderCancelApplied_unknownOrder_advancesCursor() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005004");
        when(ordersRepository.findById(orderId)).thenReturn(Optional.empty());

        projector.applyOrderCancelAppliedEvent(sampleEvent(orderId, 1), FRAGMENT_POSITION);

        verify(cursorRepository).advance(
                OmsPostgresProjector.PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                FRAGMENT_POSITION);
    }

    private static OrderCancelAppliedEvent sampleEvent(UUID orderId, int newVersion) {
        return new OrderCancelAppliedEvent(
                orderId,
                CANCELLED_AT_MS,
                newVersion,
                /* shardId = */ 0,
                "acct",
                "hash",
                "AAPL",
                "ledger_inflight_hold_failed:insufficient_funds");
    }

    private static Order orderAt(UUID id, int version, OrderStatus status) {
        return new Order(
                id,
                /* accountId = */ UUID.fromString("11111111-1111-4111-8111-111111111111"),
                /* clientIdempotencyKey = */ "idem",
                /* shardId = */ 0,
                version,
                status,
                /* terminalReason = */ null,
                Side.BUY,
                "AAPL",
                BigDecimal.valueOf(10),
                /* limitPrice = */ null,
                "DAY",
                Instant.ofEpochMilli(CANCELLED_AT_MS - 1000L),
                Instant.ofEpochMilli(CANCELLED_AT_MS - 999L),
                /* terminalAt = */ null,
                "hash",
                /* ledgerBalanceId = */ null,
                /* cumFilledQuantity = */ BigDecimal.ZERO);
    }
}
