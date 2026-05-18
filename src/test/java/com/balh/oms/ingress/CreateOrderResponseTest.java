package com.balh.oms.ingress;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wed-demo (d1_read_dto_qtys): verify {@code cumFilledQuantity} + {@code leavesQuantity}
 * appear on the read DTO and stay numerically consistent across the order lifecycle the
 * trader desk + customer FE actually render:
 *
 * <ul>
 *   <li>fresh admit (cumFilled=0) → leaves=quantity;</li>
 *   <li>partial fill → leaves=qty-cum, both non-zero;</li>
 *   <li>full fill → leaves=0 (and the consumer can match against status=FILLED);</li>
 *   <li>null {@code cumFilledQuantity} (legacy DB row) → defaults to zero so the JSON
 *       payload never carries a {@code null}.</li>
 * </ul>
 */
class CreateOrderResponseTest {

    @Test
    void freshAdmit_leavesEqualsQuantity_cumIsZero() {
        Order o = baseOrder()
                .quantity(new BigDecimal("100"))
                .cumFilledQuantity(BigDecimal.ZERO)
                .status(OrderStatus.WORKING)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o);

        assertThat(dto.cumFilledQuantity()).isEqualByComparingTo("0");
        assertThat(dto.leavesQuantity()).isEqualByComparingTo("100");
    }

    @Test
    void partialFill_leavesEqualsRemainder() {
        Order o = baseOrder()
                .quantity(new BigDecimal("100"))
                .cumFilledQuantity(new BigDecimal("37.5"))
                .status(OrderStatus.PARTIALLY_FILLED)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o);

        assertThat(dto.cumFilledQuantity()).isEqualByComparingTo("37.5");
        assertThat(dto.leavesQuantity()).isEqualByComparingTo("62.5");
    }

    @Test
    void fullFill_leavesZero_statusFilled() {
        Order o = baseOrder()
                .quantity(new BigDecimal("100"))
                .cumFilledQuantity(new BigDecimal("100"))
                .status(OrderStatus.FILLED)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o);

        assertThat(dto.cumFilledQuantity()).isEqualByComparingTo("100");
        assertThat(dto.leavesQuantity()).isEqualByComparingTo("0");
        assertThat(dto.status()).isEqualTo(OrderStatus.FILLED);
    }

    @Test
    void nullCumFilled_treatedAsZero() {
        // Defensive: legacy projector data could in principle hold a null cum_filled_quantity
        // row. The DTO must not propagate that null onto the wire (frontend code paths assume
        // a number for the progress bar arithmetic).
        Order o = baseOrder()
                .quantity(new BigDecimal("50"))
                .cumFilledQuantity(null)
                .status(OrderStatus.WORKING)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o);

        assertThat(dto.cumFilledQuantity()).isEqualByComparingTo("0");
        assertThat(dto.leavesQuantity()).isEqualByComparingTo("50");
    }

    @Test
    void overfill_clampsLeavesToZero_notNegative() {
        // Should never happen — cluster guards against cumFilled > quantity at admission. But if
        // a wire-format bug ever ships an over-fill, the DTO must clamp rather than render a
        // negative progress bar in the trading desk.
        Order o = baseOrder()
                .quantity(new BigDecimal("100"))
                .cumFilledQuantity(new BigDecimal("101"))
                .status(OrderStatus.FILLED)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o);

        assertThat(dto.leavesQuantity()).isEqualByComparingTo("0");
    }

    @Test
    void settlementStatus_surfacedSeparately_doesNotAffectQtyMath() {
        Order o = baseOrder()
                .quantity(new BigDecimal("10"))
                .cumFilledQuantity(new BigDecimal("4"))
                .status(OrderStatus.PARTIALLY_FILLED)
                .build();

        CreateOrderResponse dto = CreateOrderResponse.from(o, "PARTIAL");

        assertThat(dto.settlementStatus()).isEqualTo("PARTIAL");
        assertThat(dto.cumFilledQuantity()).isEqualByComparingTo("4");
        assertThat(dto.leavesQuantity()).isEqualByComparingTo("6");
    }

    private static OrderBuilder baseOrder() {
        return new OrderBuilder();
    }

    /** Local mini-builder to keep the per-test boilerplate tight on the Order record. */
    private static final class OrderBuilder {
        private OrderStatus status = OrderStatus.WORKING;
        private BigDecimal quantity = BigDecimal.ONE;
        private BigDecimal cumFilled = BigDecimal.ZERO;

        OrderBuilder status(OrderStatus s) { this.status = s; return this; }
        OrderBuilder quantity(BigDecimal q) { this.quantity = q; return this; }
        OrderBuilder cumFilledQuantity(BigDecimal c) { this.cumFilled = c; return this; }

        Order build() {
            return new Order(
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    "ck-" + UUID.randomUUID(),
                    0, 1,
                    status, null,
                    Side.BUY, "AAPL",
                    quantity, new BigDecimal("100"), "DAY",
                    Instant.now(), Instant.now(), null,
                    "acct-hash", "bal-1",
                    cumFilled);
        }
    }
}
