package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.ControlPostgresWritePath;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControlTailerRejectPublisherTest {

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
    void staleRejectInsertsOrderRejectedAfterSuccessfulCas() throws Exception {
        PendingControlEvent ev = sampleEvent();
        when(stale.isStale(ev.orderTimestamp())).thenReturn(true);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_STALE_QUEUE),
                isNull(),
                any(Instant.class))).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.STALE_REJECTED);

        verify(routeDispatcher, never()).enqueueWorkingOrder(any());
        verify(controlDecisions).record(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq("REJECT"),
                eq(RejectCode.RISK_STALE_QUEUE),
                eq(ControlRiskEvaluator.STAGE_CONTROL),
                isNull());
        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(domainOutbox).insert(eq(ev.orderId()), cap.capture());
        JsonNode root = objectMapper.readTree(cap.getValue());
        assertThat(root.path("type").asText()).isEqualTo("OrderRejected");
        assertThat(root.path("payload").path("orderId").asText()).isEqualTo(ev.orderId().toString());
        assertThat(root.path("payload").path("eventSeq").asInt()).isEqualTo(ev.orderVersion() + 1);
        assertThat(root.path("payload").path("rejectCode").asText()).isEqualTo(RejectCode.RISK_STALE_QUEUE.name());
        assertThat(meterRegistry.counter("oms_order_rejected_events_published_total", "reject_code", "RISK_STALE_QUEUE").count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("oms_control_jobs_rejected_stale_total").count()).isEqualTo(1.0);
    }

    @Test
    void staleRejectSkipsOutboxWhenCasDoesNotApply() {
        PendingControlEvent ev = sampleEvent();
        when(stale.isStale(ev.orderTimestamp())).thenReturn(true);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_STALE_QUEUE),
                isNull(),
                any(Instant.class))).thenReturn(false);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH);
        verify(domainOutbox, never()).insert(any(), any());
        verify(routeDispatcher, never()).enqueueWorkingOrder(any());
        verify(controlDecisions, never()).record(any(), anyInt(), anyString(), any(), anyString(), any());
    }

    @Test
    void buyingPowerRejectInsertsOrderRejected() throws Exception {
        when(config.getLedger()).thenReturn(ledgerCfg);
        when(ledgerCfg.isEnabled()).thenReturn(true);
        Order row = sampleOrder(evOrderId());
        when(orders.findById(row.id())).thenReturn(Optional.of(row));
        when(controlRisk.evaluate(row)).thenReturn(Optional.empty());
        when(buyingPower.evaluate(row)).thenReturn(BuyingPowerAdmission.Outcome.REJECT_INSUFFICIENT);
        PendingControlEvent ev = eventForOrder(row.id());
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_BUYING_POWER),
                isNull(),
                any(Instant.class))).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.BUYING_POWER_REJECTED);

        verify(routeDispatcher, never()).enqueueWorkingOrder(any());
        verify(controlDecisions).record(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq("REJECT"),
                eq(RejectCode.RISK_BUYING_POWER),
                eq(ControlRiskEvaluator.STAGE_CONTROL),
                isNull());
        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(domainOutbox).insert(eq(ev.orderId()), cap.capture());
        assertThat(objectMapper.readTree(cap.getValue()).path("payload").path("rejectCode").asText())
                .isEqualTo(RejectCode.RISK_BUYING_POWER.name());
    }

    private static UUID evOrderId() {
        return UUID.fromString("11111111-1111-1111-1111-111111111111");
    }

    private static PendingControlEvent eventForOrder(UUID orderId) {
        Instant ts = Instant.parse("2026-05-07T12:00:00Z");
        return new PendingControlEvent("OrderAccepted", orderId, 0, 0, "accthash", ts, ts);
    }

    private static PendingControlEvent sampleEvent() {
        return eventForOrder(UUID.fromString("22222222-2222-2222-2222-222222222222"));
    }

    private static Order sampleOrder(UUID id) {
        Instant t = Instant.now();
        return new Order(
                id,
                UUID.randomUUID(),
                "k",
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("3"),
                new BigDecimal("5"),
                "DAY",
                t,
                t,
                null,
                "hash",
                "balance_x",
                BigDecimal.ZERO);
    }
}
