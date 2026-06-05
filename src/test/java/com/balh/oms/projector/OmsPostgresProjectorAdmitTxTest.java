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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.TimeUnit;

/**
 * Admit-transaction profiling contract: {@link OmsPostgresProjector#TIMER_ADMIT_TX} service-time
 * histogram and the PREDMKT bench skip of {@code control_decisions} PASS rows.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorAdmitTxTest {

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
        projector.setCurrentRecordingIdForTesting(13L);
    }

    @Test
    void applyAdmittedEvent_recordsAdmitTxServiceTimer() throws Exception {
        OrderAdmittedEvent ev = predmktAdmitted();
        Order projected = org.mockito.Mockito.mock(Order.class);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(true, projected));
        when(envelopeCodec.orderAcceptedFromAdmitted(ev)).thenReturn("{\"type\":\"OrderAccepted\"}");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        Timer timer = meterRegistry.find(OmsPostgresProjector.TIMER_ADMIT_TX).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void benchSkipEnabled_predmktSymbol_passesSkipFlagToControlAdmission() throws Exception {
        projector.setSkipVenueControlPassAuditForTesting(true);
        OrderAdmittedEvent ev = predmktAdmitted();
        Order projected = org.mockito.Mockito.mock(Order.class);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(true, projected));
        when(envelopeCodec.orderAcceptedFromAdmitted(ev)).thenReturn("{\"type\":\"OrderAccepted\"}");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(controlAdmission).persistAdmission(any(), eq(projected), eq(true));
        verify(ordersRepository, never()).orderFromAdmittedEvent(any());
    }

    @Test
    void benchSkipEnabled_equitySymbol_doesNotSkipPassAudit() throws Exception {
        projector.setSkipVenueControlPassAuditForTesting(true);
        OrderAdmittedEvent ev = equityAdmitted();
        Order projected = org.mockito.Mockito.mock(Order.class);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(true, projected));
        when(envelopeCodec.orderAcceptedFromAdmitted(ev)).thenReturn("{\"type\":\"OrderAccepted\"}");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(controlAdmission).persistAdmission(any(), eq(projected), eq(false));
    }

    @Test
    void replay_stillRecordsAdmitTxTimerForCursorAdvance() {
        OrderAdmittedEvent ev = predmktAdmitted();
        Order projected = org.mockito.Mockito.mock(Order.class);
        when(ordersRepository.insertFromAdmittedEventWithOrder(ev))
                .thenReturn(new OrdersRepository.ProjectorAdmitInsert(false, projected));

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(controlAdmission, never()).persistAdmission(any(), any(), any(Boolean.class));
        Timer timer = meterRegistry.find(OmsPostgresProjector.TIMER_ADMIT_TX).timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
    }

    private static OrderAdmittedEvent predmktAdmitted() {
        return admitted("PREDMKT-TEST-1");
    }

    private static OrderAdmittedEvent equityAdmitted() {
        return admitted("AAPL");
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
                "idem",
                "hash",
                symbol,
                null);
    }
}
