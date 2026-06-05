package com.balh.oms.projector;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
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
 * Async BUY hold is now placed synchronously before cluster admit, so projector-side outbox
 * projection must stay disabled to avoid a second Ledger POST.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorD1BackfillTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long FRAGMENT_POSITION = 8192L;

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

    private OmsConfig config;
    private OmsPostgresProjector projector;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 1L), ZoneOffset.UTC);
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
                objectMapper,
                txManager,
                pinned,
                new com.balh.oms.settlement.SettlementDateCalculator(
                        com.balh.oms.settlement.SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK),
                org.mockito.Mockito.mock(com.balh.oms.settlement.PredictionMarketResolutionService.class),
                null);
        // Production seeding (init() → bootstrap → replay loop) is bypassed in unit tests that
        // drive applyAdmittedEvent directly. Seed the recording id so the apply path's cursor
        // write does not fail loud on -1.
        projector.setCurrentRecordingIdForTesting(13L);
        // D-1 tests target ledger_inflight_outbox only; treat admits as replay so the fresh-insert
        // branch (OrderAccepted envelope + control admission) stays out of scope.
        when(ordersRepository.insertFromAdmittedEventWithOrder(any()))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(
                        false, org.mockito.Mockito.mock(com.balh.oms.domain.Order.class)));
    }

    @Test
    void inflightReservationDisabled_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(false);
        config.getLedger().setInflightAsyncEnabled(true);

        projector.applyAdmittedEvent(buyAdmittedWithLedger(123L, "10000"), FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void inflightAsyncDisabled_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(false);

        projector.applyAdmittedEvent(buyAdmittedWithLedger(123L, "10000"), FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void sellSide_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_SELL,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 100_000L,
                "ledger-bal-1");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void noLedgerBalanceId_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 100_000L,
                /* ledgerBalanceId = */ null);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void marketOrder_zeroLimitPrice_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 0L,
                "ledger-bal-1");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void buyAsyncWithLedgerAndLimit_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        long quantityScaled = 12_345_000_000L; // 12.345
        long limitPriceScaled = 100_500_000L;  // 100.5
        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                quantityScaled,
                limitPriceScaled,
                "ledger-bal-d1");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void replayPath_stillAdvancesCursorWithoutProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = buyAdmittedWithLedger(/* quantityScaled = */ 1_000_000_000L, /* limit = */ "100");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
        verify(cursorRepository).advanceWithRecording(anyString(), anyInt(), eq(13L), eq(FRAGMENT_POSITION));
    }

    private static OrderAdmittedEvent admitted(
            byte side, long quantityScaled, long limitPriceScaled, String ledgerBalanceId) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                quantityScaled,
                limitPriceScaled,
                /* shardId = */ 0,
                /* version = */ 0,
                side,
                AcceptOrderCommand.TIF_DAY,
                "00000000-0000-4000-8000-00000000d1a1",
                "idem-" + UUID.randomUUID(),
                "hash",
                "AAPL",
                ledgerBalanceId);
    }

    private static OrderAdmittedEvent buyAdmittedWithLedger(long quantityScaled, String limitPlain) {
        long limitPriceScaled = new java.math.BigDecimal(limitPlain)
                .movePointRight(6)
                .longValueExact();
        return admitted(AcceptOrderCommand.SIDE_BUY, quantityScaled, limitPriceScaled, "ledger-bal-1");
    }
}
