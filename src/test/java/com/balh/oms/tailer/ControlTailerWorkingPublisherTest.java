package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.ControlPostgresWritePath;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.ControlDecisionsRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.FixNosRouteEnqueueClaimRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
import com.balh.oms.risk.ControlRiskEvaluator;
import com.balh.oms.routing.RouteDispatcher;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControlTailerWorkingPublisherTest {

    @Mock OrdersRepository orders;
    @Mock StaleJobGuard stale;
    @Mock OmsConfig config;
    @Mock OmsConfig.Control controlCfg;
    @Mock OmsConfig.Ledger ledgerCfg;
    @Mock BuyingPowerAdmission buyingPower;
    @Mock ControlRiskEvaluator controlRisk;
    @Mock ControlDecisionsRepository controlDecisions;
    @Mock DomainEventOutboxRepository domainOutbox;
    @Mock RouteDispatcher routeDispatcher;
    @Mock IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;
    @Mock FixNosRouteEnqueueClaimRepository fixNosRouteEnqueueClaimRepository;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final DomainEventEnvelopeCodec codec = new DomainEventEnvelopeCodec(objectMapper);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    ControlTailer tailer;

    @BeforeEach
    void setUp() {
        when(config.getControl()).thenReturn(controlCfg);
        when(controlCfg.getPostgresWritePath()).thenReturn(ControlPostgresWritePath.TAIL);
        OrderControlAdmission admission = new OrderControlAdmission(
                orders,
                stale,
                config,
                buyingPower,
                controlRisk,
                controlDecisions,
                domainOutbox,
                codec,
                meterRegistry,
                ingressToFixNosLatencyRecorder);
        tailer = new ControlTailer(
                admission, orders, stale, config, meterRegistry, routeDispatcher, fixNosRouteEnqueueClaimRepository);
    }

    @Test
    void workingTransitionInsertsOrderWorkingEnvelopeAfterSuccessfulCas() throws Exception {
        when(config.getLedger()).thenReturn(ledgerCfg);
        when(ledgerCfg.isEnabled()).thenReturn(false);
        PendingControlEvent ev = new PendingControlEvent(
                "OrderAccepted",
                UUID.fromString("33333333-3333-3333-3333-333333333333"),
                0,
                0,
                "accthash",
                Instant.parse("2026-05-07T10:00:00Z"),
                Instant.parse("2026-05-07T10:00:01Z"));
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        Order beforeCas = sampleOrder(ev.orderId(), 0);
        Order afterCas = sampleOrder(ev.orderId(), 1);
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(beforeCas)).thenReturn(Optional.of(afterCas));
        when(controlRisk.evaluate(beforeCas)).thenReturn(Optional.empty());
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.WORKING),
                isNull(),
                eq(ev.orderTimestamp()),
                isNull())).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        verify(controlDecisions).record(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq("PASS"),
                isNull(),
                eq(ControlRiskEvaluator.STAGE_CONTROL),
                isNull());
        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(domainOutbox).insert(eq(ev.orderId()), cap.capture());
        JsonNode root = objectMapper.readTree(cap.getValue());
        assertThat(root.path("type").asText()).isEqualTo("OrderWorking");
        assertThat(root.path("payload").path("orderId").asText()).isEqualTo(ev.orderId().toString());
        assertThat(root.path("payload").path("eventSeq").asInt()).isEqualTo(1);
        assertThat(root.path("payload").path("side").asText()).isEqualTo("BUY");
        assertThat(meterRegistry.counter("oms_order_working_events_published_total").count()).isEqualTo(1.0);
        verify(routeDispatcher).enqueueWorkingOrder(eq(ev.orderId()));
    }

    private static Order sampleOrder(UUID id, int version) {
        Instant t = Instant.parse("2026-05-07T10:00:00Z");
        return new Order(
                id,
                UUID.randomUUID(),
                "k",
                0,
                version,
                OrderStatus.NEW,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("2"),
                new BigDecimal("3"),
                "DAY",
                t,
                t,
                null,
                "hash",
                null,
                BigDecimal.ZERO);
    }
}
