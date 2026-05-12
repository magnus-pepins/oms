package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.ControlPostgresWritePath;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.observability.metrics.OmsPipelineMeterNames;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.ControlDecisionsRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.FixNosRouteEnqueueClaimRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
import com.balh.oms.risk.ControlRiskEvaluator;
import com.balh.oms.routing.RouteDispatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code oms.control.postgres-write-path=ingress}: tail must not mutate Postgres; only enqueue routing when
 * {@code WORKING} at {@code event.orderVersion() + 1}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ControlTailerIngressDispatchOnlyTest {

    @Mock OrdersRepository orders;
    @Mock StaleJobGuard stale;
    @Mock OmsConfig config;
    @Mock OmsConfig.Control controlCfg;
    @Mock OmsConfig.Routing routingCfg;
    @Mock BuyingPowerAdmission buyingPower;
    @Mock ControlRiskEvaluator controlRisk;
    @Mock ControlDecisionsRepository controlDecisions;
    @Mock DomainEventOutboxRepository domainOutbox;
    @Mock RouteDispatcher routeDispatcher;
    @Mock IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;
    @Mock FixNosRouteEnqueueClaimRepository fixNosRouteEnqueueClaimRepository;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final DomainEventEnvelopeCodec codec = new DomainEventEnvelopeCodec(objectMapper);

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private ControlTailer tailer;

    @BeforeEach
    void setUp() {
        when(config.getControl()).thenReturn(controlCfg);
        when(controlCfg.getPostgresWritePath()).thenReturn(ControlPostgresWritePath.INGRESS);
        when(config.getRouting()).thenReturn(routingCfg);
        when(routingCfg.getBackend()).thenReturn("fix");
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
    void workingAtExpectedVersion_enqueuesRouteWithoutCas() {
        PendingControlEvent ev = new PendingControlEvent(
                "OrderAccepted",
                UUID.fromString("44444444-4444-4444-4444-444444444444"),
                0,
                0,
                "accthash",
                Instant.parse("2026-05-07T10:00:00Z"),
                Instant.parse("2026-05-07T10:00:01Z"));
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        Order working = sampleOrder(ev.orderId(), 1, OrderStatus.WORKING);
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(working));
        when(fixNosRouteEnqueueClaimRepository.tryClaim(ev.orderId())).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        verify(orders, never()).updateWithCas(
                eq(ev.orderId()), eq(ev.orderVersion()), eq(OrderStatus.WORKING), eq(null), eq(ev.orderTimestamp()), eq(null));
        verify(fixNosRouteEnqueueClaimRepository).tryClaim(ev.orderId());
        verify(routeDispatcher).enqueueWorkingOrder(eq(ev.orderId()));
    }

    @Test
    void enqueueClaimAlreadyHeld_skipsRouteAndIncrementsMetric() {
        PendingControlEvent ev = new PendingControlEvent(
                "OrderAccepted",
                UUID.fromString("66666666-6666-6666-6666-666666666666"),
                0,
                0,
                "accthash",
                Instant.parse("2026-05-07T10:00:00Z"),
                Instant.parse("2026-05-07T10:00:01Z"));
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        Order working = sampleOrder(ev.orderId(), 1, OrderStatus.WORKING);
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(working));
        when(fixNosRouteEnqueueClaimRepository.tryClaim(ev.orderId())).thenReturn(false);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH);

        verify(routeDispatcher, never()).enqueueWorkingOrder(ev.orderId());
        assertThat(meterRegistry.counter(OmsPipelineMeterNames.CONTROL_INGRESS_DISPATCH_ENQUEUE_CLAIM_SKIP).count())
                .isEqualTo(1.0);
    }

    @Test
    void stillNew_skipsDispatch() {
        PendingControlEvent ev = new PendingControlEvent(
                "OrderAccepted",
                UUID.fromString("55555555-5555-5555-5555-555555555555"),
                0,
                0,
                "accthash",
                Instant.parse("2026-05-07T10:00:00Z"),
                Instant.parse("2026-05-07T10:00:01Z"));
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        Order stillNew = sampleOrder(ev.orderId(), 0, OrderStatus.NEW);
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(stillNew));

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH);
        verify(routeDispatcher, never()).enqueueWorkingOrder(ev.orderId());
        verify(fixNosRouteEnqueueClaimRepository, never()).tryClaim(any());
    }

    @Test
    void noopBackend_skipsClaimInsert() {
        when(routingCfg.getBackend()).thenReturn("noop");
        PendingControlEvent ev = new PendingControlEvent(
                "OrderAccepted",
                UUID.fromString("77777777-7777-7777-7777-777777777777"),
                0,
                0,
                "accthash",
                Instant.parse("2026-05-07T10:00:00Z"),
                Instant.parse("2026-05-07T10:00:01Z"));
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        Order working = sampleOrder(ev.orderId(), 1, OrderStatus.WORKING);
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(working));

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        verify(fixNosRouteEnqueueClaimRepository, never()).tryClaim(any());
        verify(routeDispatcher).enqueueWorkingOrder(eq(ev.orderId()));
    }

    private static Order sampleOrder(UUID id, int version, OrderStatus status) {
        Instant t = Instant.parse("2026-05-07T10:00:00Z");
        return new Order(
                id,
                UUID.randomUUID(),
                "k",
                0,
                version,
                status,
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
