package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Phase 4 Tier 2.5 phase D-3 — covers the {@code OrderAccepted} envelope emission moved from
 * {@code OrderIngressService} into
 * {@link OmsPostgresProjector#applyAdmittedEvent(OrderAdmittedEvent, long)}.
 *
 * <p>Pinned contract:
 * <ul>
 *   <li>On a fresh admission ({@link OrdersRepository#insertFromAdmittedEvent} returns
 *       {@code true}), exactly one {@code OrderAccepted} envelope is inserted into
 *       {@code domain_event_outbox} via the codec's
 *       {@link DomainEventEnvelopeCodec#orderAcceptedFromAdmitted(OrderAdmittedEvent)}.</li>
 *   <li>On a replay ({@code insertFromAdmittedEvent} returns {@code false}), no envelope is
 *       written — the original projection's row stands. This protects fanout from duplicate
 *       events on cursor rewind / recording-rebuild.</li>
 *   <li>The codec is called with the same {@link OrderAdmittedEvent} the projector received,
 *       so {@code orders} row content and {@code domain_event_outbox} envelope content are
 *       always derived from one authoritative source — no field drift between the two paths.</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorD3OrderAcceptedTest {

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
    // Mirror the Spring auto-configured ObjectMapper that DomainEventEnvelopeCodec sees in
    // production: JavaTimeModule registered so OrderAcceptedEvent.acceptedAt (java.time.Instant)
    // serialises without an InvalidDefinitionException.
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        // D-3 + D-1 are independent: leave inflight flags off so the only side-effect we
        // assert in this class is the OrderAccepted envelope, not the ledger backfill.
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 1L), ZoneOffset.UTC);
        // Use a real DomainEventEnvelopeCodec when we want to verify payload shape; otherwise
        // the @Mock instance gives us interaction-only assertions. The fresh-admission test
        // below swaps the projector to use a real codec to capture the actual JSON.
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
                        com.balh.oms.settlement.SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK));
    }

    @Test
    void freshAdmission_writesOrderAcceptedEnvelope_andCallsCodecWithSameEvent() throws Exception {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY);
        when(ordersRepository.insertFromAdmittedEvent(ev)).thenReturn(true);
        when(envelopeCodec.orderAcceptedFromAdmitted(ev)).thenReturn("{\"type\":\"OrderAccepted\"}");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(envelopeCodec, times(1)).orderAcceptedFromAdmitted(ev);
        verify(domainEventOutboxRepository, times(1))
                .insert(eq(ev.orderId()), eq("{\"type\":\"OrderAccepted\"}"));
    }

    @Test
    void replay_insertReturnsFalse_doesNotEmitOrderAcceptedEnvelope() throws Exception {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_SELL, AcceptOrderCommand.TIF_GTC);
        when(ordersRepository.insertFromAdmittedEvent(ev)).thenReturn(false);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(envelopeCodec, never()).orderAcceptedFromAdmitted(any());
        verify(domainEventOutboxRepository, never()).insert(any(), any());
    }

    @Test
    void freshAdmission_realCodec_payloadMatchesEventFields() throws Exception {
        // Re-wire the projector with a real codec so we can capture and parse the JSON. We
        // keep the rest of the collaborators as mocks — only the codec needs real serialisation
        // logic to make the assertion meaningful.
        // Phase 4 Tier 2.5 phase E-2: the event below carries shardId=7 so we can pin "the
        // payload's shardId field is the event's shardId field, not a hard-coded 0". Move the
        // projector's own shard id to 7 too so the new defensive shard guard does not drop
        // the event before applyAdmittedEvent runs.
        config.getShard().setId(7);
        DomainEventEnvelopeCodec realCodec = new DomainEventEnvelopeCodec(objectMapper);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 1L), ZoneOffset.UTC);
        OmsPostgresProjector projectorWithRealCodec = new OmsPostgresProjector(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                ledgerInflightOutboxRepository,
                realCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                objectMapper,
                txManager,
                pinned,
                new com.balh.oms.settlement.SettlementDateCalculator(
                        com.balh.oms.settlement.SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK));

        OrderAdmittedEvent ev = new OrderAdmittedEvent(
                UUID.fromString("00000000-0000-4000-8000-0000000000d3"),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 12_345_000_000L,   // 12.345
                /* limitPriceScaledOrZero = */ 100_500_000L, // 100.5
                /* shardId = */ 7,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct-d3",
                "idem-d3",
                "hash-d3",
                "AAPL",
                "ledger-bal-d3");
        when(ordersRepository.insertFromAdmittedEvent(ev)).thenReturn(true);

        projectorWithRealCodec.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(domainEventOutboxRepository, times(1))
                .insert(eq(ev.orderId()), payloadCaptor.capture());
        JsonNode envelope = objectMapper.readTree(payloadCaptor.getValue());
        assertThat(envelope.get("type").asText()).isEqualTo("OrderAccepted");
        assertThat(envelope.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(envelope.get("correlationId").asText()).isEqualTo(ev.orderId().toString());
        JsonNode payload = envelope.get("payload");
        assertThat(payload.get("orderId").asText()).isEqualTo(ev.orderId().toString());
        assertThat(payload.get("side").asText()).isEqualTo("BUY");
        assertThat(payload.get("instrumentSymbol").asText()).isEqualTo("AAPL");
        assertThat(payload.get("timeInForce").asText()).isEqualTo("DAY");
        assertThat(new java.math.BigDecimal(payload.get("quantity").asText()))
                .isEqualByComparingTo(new java.math.BigDecimal("12.345"));
        assertThat(new java.math.BigDecimal(payload.get("limitPrice").asText()))
                .isEqualByComparingTo(new java.math.BigDecimal("100.5"));
        assertThat(payload.get("shardId").asInt()).isEqualTo(7);
        assertThat(payload.get("accountIdHash").asText()).isEqualTo("hash-d3");
    }

    @Test
    void freshAdmission_marketOrder_limitPriceSerialisedAsNull() throws Exception {
        DomainEventEnvelopeCodec realCodec = new DomainEventEnvelopeCodec(objectMapper);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 1L), ZoneOffset.UTC);
        OmsPostgresProjector projectorWithRealCodec = new OmsPostgresProjector(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                ledgerInflightOutboxRepository,
                realCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                objectMapper,
                txManager,
                pinned,
                new com.balh.oms.settlement.SettlementDateCalculator(
                        com.balh.oms.settlement.SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK));

        OrderAdmittedEvent ev = new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 1_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L, // market
                /* shardId = */ 0,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_IOC,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
        when(ordersRepository.insertFromAdmittedEvent(ev)).thenReturn(true);

        projectorWithRealCodec.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(domainEventOutboxRepository, times(1))
                .insert(eq(ev.orderId()), payloadCaptor.capture());
        JsonNode envelope = objectMapper.readTree(payloadCaptor.getValue());
        JsonNode payload = envelope.get("payload");
        assertThat(payload.has("limitPrice")).isTrue();
        assertThat(payload.get("limitPrice").isNull())
                .as("market orders (limitPriceScaled=0) must serialise as JSON null so downstream"
                        + " consumers see the same shape ingress's previous OrderAccepted produced")
                .isTrue();
    }

    private static OrderAdmittedEvent sampleAdmitted(byte side, byte tif) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaledOrZero = */ 100_000_000L,
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
