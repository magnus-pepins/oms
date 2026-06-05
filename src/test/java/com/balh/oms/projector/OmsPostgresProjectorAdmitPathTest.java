package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.persistence.OrdersRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Pins the in-memory {@link Order} view the projector passes into control admission so the hot
 * path does not re-SELECT the row it just inserted.
 */
class OmsPostgresProjectorAdmitPathTest {

    @Test
    void orderFromAdmittedEvent_matchesInsertProjectionFields() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-0000000000ad");
        OrderAdmittedEvent ev = new OrderAdmittedEvent(
                orderId,
                /* clientTimestampNanos = */ 1_700_000_000_123_456_789L,
                /* acceptedAtMillis = */ 1_700_000_000_000L,
                /* quantityScaled = */ 10_500_000_000L,
                /* limitPriceScaledOrZero = */ 30_000L,
                /* shardId = */ 0,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                AcceptOrderCommand.ORD_TYPE_LIMIT,
                "00000000-0000-4000-8000-00000000acc1",
                "idem-admit-path",
                "hash-admit",
                "PREDMKT-TEST",
                /* ledgerBalanceIdOrNull = */ "bal-1");

        // orderFromAdmittedEvent is pure mapping; no JDBC on this path.
        Order order = new OrdersRepository(null).orderFromAdmittedEvent(ev);

        assertThat(order.id()).isEqualTo(orderId);
        assertThat(order.accountId()).isEqualTo(UUID.fromString("00000000-0000-4000-8000-00000000acc1"));
        assertThat(order.clientIdempotencyKey()).isEqualTo("idem-admit-path");
        assertThat(order.shardId()).isZero();
        assertThat(order.version()).isZero();
        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_NEW);
        assertThat(order.terminalReason()).isNull();
        assertThat(order.side()).isEqualTo(Side.BUY);
        assertThat(order.instrumentSymbol()).isEqualTo("PREDMKT-TEST");
        assertThat(order.quantity()).isEqualByComparingTo(new BigDecimal("10.5"));
        assertThat(order.limitPrice()).isEqualByComparingTo(new BigDecimal("0.03"));
        assertThat(order.timeInForce()).isEqualTo("DAY");
        assertThat(order.receivedAt()).isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_789));
        assertThat(order.acceptedAt()).isEqualTo(Instant.ofEpochMilli(1_700_000_000_000L));
        assertThat(order.terminalAt()).isNull();
        assertThat(order.accountIdHash()).isEqualTo("hash-admit");
        assertThat(order.ledgerBalanceId()).isEqualTo("bal-1");
        assertThat(order.cumFilledQuantity()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(order.ordType()).isEqualTo("LIMIT");
    }
}
