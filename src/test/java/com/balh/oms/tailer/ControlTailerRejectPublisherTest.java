package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventPublisher;
import com.balh.oms.events.OrderRejectedEvent;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControlTailerRejectPublisherTest {

    @Mock OrdersRepository orders;
    @Mock StaleJobGuard stale;
    @Mock OmsConfig config;
    @Mock OmsConfig.Ledger ledgerCfg;
    @Mock BuyingPowerAdmission buyingPower;
    @Mock DomainEventPublisher events;

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    ControlTailer tailer;

    @BeforeEach
    void setUp() {
        tailer = new ControlTailer(orders, stale, config, buyingPower, events, meterRegistry);
    }

    @Test
    void staleRejectPublishesOrderRejectedAfterSuccessfulCas() {
        PendingControlEvent ev = sampleEvent();
        when(stale.isStale(ev.orderTimestamp())).thenReturn(true);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_STALE_QUEUE),
                eq(null),
                any(Instant.class))).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.STALE_REJECTED);

        ArgumentCaptor<OrderRejectedEvent> cap = ArgumentCaptor.forClass(OrderRejectedEvent.class);
        verify(events).publish(cap.capture());
        OrderRejectedEvent published = cap.getValue();
        assertThat(published.orderId()).isEqualTo(ev.orderId());
        assertThat(published.eventSeq()).isEqualTo(ev.orderVersion() + 1);
        assertThat(published.rejectCode()).isEqualTo(RejectCode.RISK_STALE_QUEUE.name());
        assertThat(meterRegistry.counter("oms_order_rejected_events_published_total", "reject_code", "RISK_STALE_QUEUE").count())
                .isEqualTo(1.0);
    }

    @Test
    void staleRejectSkipsPublishWhenCasDoesNotApply() {
        PendingControlEvent ev = sampleEvent();
        when(stale.isStale(ev.orderTimestamp())).thenReturn(true);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_STALE_QUEUE),
                eq(null),
                any(Instant.class))).thenReturn(false);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH);
        verify(events, never()).publish(any());
    }

    @Test
    void buyingPowerRejectPublishesOrderRejected() {
        when(config.getLedger()).thenReturn(ledgerCfg);
        when(ledgerCfg.isEnabled()).thenReturn(true);
        Order row = sampleOrder(evOrderId());
        when(orders.findById(row.id())).thenReturn(Optional.of(row));
        when(buyingPower.evaluate(row)).thenReturn(BuyingPowerAdmission.Outcome.REJECT_INSUFFICIENT);
        PendingControlEvent ev = eventForOrder(row.id());
        when(stale.isStale(ev.orderTimestamp())).thenReturn(false);
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.REJECTED),
                eq(RejectCode.RISK_BUYING_POWER),
                eq(null),
                any(Instant.class))).thenReturn(true);

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.BUYING_POWER_REJECTED);

        ArgumentCaptor<OrderRejectedEvent> cap = ArgumentCaptor.forClass(OrderRejectedEvent.class);
        verify(events).publish(cap.capture());
        assertThat(cap.getValue().rejectCode()).isEqualTo(RejectCode.RISK_BUYING_POWER.name());
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
                "balance_x");
    }
}
