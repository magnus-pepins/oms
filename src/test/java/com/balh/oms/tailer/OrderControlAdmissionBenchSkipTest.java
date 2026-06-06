package com.balh.oms.tailer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.ControlDecisionsRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
import com.balh.oms.risk.ControlRiskEvaluator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderControlAdmissionBenchSkipTest {

    @Mock private OrdersRepository orders;
    @Mock private StaleJobGuard stale;
    @Mock private BuyingPowerAdmission buyingPower;
    @Mock private ControlRiskEvaluator controlRisk;
    @Mock private ControlDecisionsRepository controlDecisions;
    @Mock private DomainEventOutboxRepository domainEventOutbox;
    @Mock private DomainEventEnvelopeCodec envelopeCodec;

    private OmsConfig config;
    private OrderControlAdmission admission;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getLedger().setEnabled(true);
        admission = new OrderControlAdmission(
                orders,
                stale,
                config,
                buyingPower,
                controlRisk,
                controlDecisions,
                domainEventOutbox,
                envelopeCodec,
                new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(null);
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(null);
    }

    @Test
    void persistAdmission_skipsBuyingPowerWhenBenchGateEnabled() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(true);
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);

        Order order = predmktOrder("bal-1");
        PendingControlEvent event = controlEvent(order.id());

        when(stale.isStale(any())).thenReturn(false);

        OrderControlAdmission.AdmissionResult result =
                admission.persistAdmission(event, order, true);

        assertThat(result).isEqualTo(OrderControlAdmission.AdmissionResult.APPLIED);
        verify(controlRisk, never()).evaluate(any());
        verify(buyingPower, never()).evaluate(any());
        verify(controlDecisions, never())
                .record(any(), anyInt(), eq("PASS"), isNull(), eq(ControlRiskEvaluator.STAGE_CONTROL), isNull());
    }

    @Test
    void persistAdmission_runsBuyingPowerWhenBenchGateDisabled() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(false);
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);

        Order order = predmktOrder("bal-2");
        PendingControlEvent event = controlEvent(order.id());

        when(stale.isStale(any())).thenReturn(false);
        when(buyingPower.evaluate(order)).thenReturn(BuyingPowerAdmission.Outcome.PROCEED);

        OrderControlAdmission.AdmissionResult result =
                admission.persistAdmission(event, order, true);

        assertThat(result).isEqualTo(OrderControlAdmission.AdmissionResult.APPLIED);
        verify(controlRisk, never()).evaluate(any());
        verify(buyingPower).evaluate(order);
    }

    private static PendingControlEvent controlEvent(UUID orderId) {
        Instant now = Instant.parse("2026-06-06T12:00:00Z");
        return new PendingControlEvent("OrderAccepted", orderId, 0, 0, "acct-hash", now, now);
    }

    private static Order predmktOrder(String ledgerBalanceId) {
        Instant now = Instant.parse("2026-06-06T12:00:00Z");
        return new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "idem",
                0,
                0,
                OrderStatus.PENDING_NEW,
                null,
                Side.BUY,
                "PREDMKT-TEST-1",
                new BigDecimal("1"),
                new BigDecimal("0.50"),
                "DAY",
                now,
                now,
                null,
                "acct-hash",
                ledgerBalanceId,
                BigDecimal.ZERO);
    }
}
