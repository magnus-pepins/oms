package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControlTailerWorkingPublisherTest {

    @Mock OrdersRepository orders;
    @Mock StaleJobGuard stale;
    @Mock OmsConfig config;
    @Mock OmsConfig.Ledger ledgerCfg;
    @Mock BuyingPowerAdmission buyingPower;
    @Mock DomainEventOutboxRepository domainOutbox;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final DomainEventEnvelopeCodec codec = new DomainEventEnvelopeCodec(objectMapper);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    ControlTailer tailer;

    @BeforeEach
    void setUp() {
        tailer = new ControlTailer(orders, stale, config, buyingPower, domainOutbox, codec, meterRegistry);
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
        when(orders.updateWithCas(
                eq(ev.orderId()),
                eq(ev.orderVersion()),
                eq(OrderStatus.WORKING),
                eq(null),
                eq(ev.orderTimestamp()),
                eq(null))).thenReturn(true);
        Order reloaded = sampleOrder(ev.orderId());
        when(orders.findById(ev.orderId())).thenReturn(Optional.of(reloaded));

        assertThat(tailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        ArgumentCaptor<String> cap = ArgumentCaptor.forClass(String.class);
        verify(domainOutbox).insert(eq(ev.orderId()), cap.capture());
        JsonNode root = objectMapper.readTree(cap.getValue());
        assertThat(root.path("type").asText()).isEqualTo("OrderWorking");
        assertThat(root.path("payload").path("orderId").asText()).isEqualTo(ev.orderId().toString());
        assertThat(root.path("payload").path("eventSeq").asInt()).isEqualTo(1);
        assertThat(root.path("payload").path("side").asText()).isEqualTo("BUY");
        assertThat(meterRegistry.counter("oms_order_working_events_published_total").count()).isEqualTo(1.0);
    }

    private static Order sampleOrder(UUID id) {
        Instant t = Instant.parse("2026-05-07T10:00:00Z");
        return new Order(
                id,
                UUID.randomUUID(),
                "k",
                0,
                1,
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
                null);
    }
}
