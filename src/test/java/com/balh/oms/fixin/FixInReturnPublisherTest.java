package com.balh.oms.fixin;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.fixin.persistence.FixInDropCopyEntitlementRepository;
import com.balh.oms.fixin.persistence.FixInOrderMapRepository;
import com.balh.oms.fixin.persistence.FixInOrderMapRow;
import com.balh.oms.fixin.persistence.FixInOutboundSentRepository;
import com.balh.oms.persistence.OrdersRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FixInReturnPublisherTest {

    @Mock private OrdersRepository ordersRepository;
    @Mock private FixInOrderMapRepository orderMapRepository;
    @Mock private FixInOutboundSentRepository outboundSentRepository;
    @Mock private FixInDropCopyEntitlementRepository dropCopyEntitlementRepository;
    @Mock private FixInExecutionReportBuilder executionReportBuilder;
    @Mock private FixInWireDelivery wireDelivery;

    @InjectMocks private FixInReturnPublisher publisher;

    @Test
    void publishExecutionApplied_sendsWhenDedupeAllows() {
        UUID orderId = UUID.randomUUID();
        UUID sessionId = UUID.randomUUID();
        when(orderMapRepository.findByOmsOrderId(orderId))
                .thenReturn(List.of(new FixInOrderMapRow(sessionId, "CL-1", orderId, null)));
        Order order = sampleOrder(orderId);
        when(ordersRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(outboundSentRepository.tryMarkSent(eq(sessionId), any())).thenReturn(true);
        when(dropCopyEntitlementRepository.findDropCopySessionIdsForAccount(order.accountId()))
                .thenReturn(List.of());
        ExecutionAppliedEvent ev = new ExecutionAppliedEvent(
                orderId,
                5_000_000_000L,
                5_000_000_000L,
                150_250_000L,
                1L,
                1L,
                1,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) OrderStatus.PARTIALLY_FILLED.ordinal(),
                (byte) 0,
                order.accountId().toString(),
                "SIM",
                "exec-1",
                "{}");
        when(executionReportBuilder.buildFromExecutionApplied(order, "CL-1", ev))
                .thenReturn(new quickfix.fix44.ExecutionReport());
        when(wireDelivery.sendToSession(eq(sessionId), any(), any(), eq(orderId))).thenReturn(true);

        assertThat(publisher.publishExecutionApplied(ev)).isTrue();
        verify(wireDelivery).sendToSession(eq(sessionId), any(), eq(FixInWireDelivery.sessionRoleOrderEntry()), eq(orderId));
    }

    private static Order sampleOrder(UUID orderId) {
        Instant now = Instant.now();
        return new Order(
                orderId,
                UUID.fromString("a0000001-0000-4000-8000-000000000002"),
                "idem",
                0,
                1,
                OrderStatus.PARTIALLY_FILLED,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("150.25"),
                "DAY",
                now,
                now,
                null,
                "hash",
                null,
                BigDecimal.ZERO,
                "LIMIT");
    }
}
