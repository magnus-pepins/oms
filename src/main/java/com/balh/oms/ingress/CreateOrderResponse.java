package com.balh.oms.ingress;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record CreateOrderResponse(
        UUID id,
        UUID accountId,
        String clientIdempotencyKey,
        int shardId,
        int version,
        OrderStatus status,
        String terminalReason,
        String side,
        String instrumentSymbol,
        BigDecimal quantity,
        BigDecimal limitPrice,
        String timeInForce,
        Instant receivedAt,
        Instant acceptedAt,
        Instant terminalAt,
        String ledgerBalanceId,
        /**
         * Aggregate of {@code TRADE} execution settlement states for this order; {@code null} when
         * there are no trade executions yet. Populated on {@code GET /internal/v1/orders/{id}} only.
         */
        String settlementStatus,
        /**
         * Wed-demo (d1_read_dto_qtys): cumulative filled quantity from venue execution reports.
         * Always populated when the order has been seen (0 on a freshly-admitted order). Drives
         * the trader-desk + customer-FE progress bar and the "Partially filled" toast.
         */
        BigDecimal cumFilledQuantity,
        /**
         * Wed-demo (d1_read_dto_qtys): {@code quantity - cumFilledQuantity}, computed server-side
         * so consumers don't drift on rounding. Always {@code >= 0}. Reaches 0 at the same moment
         * the status flips to {@code FILLED}.
         */
        BigDecimal leavesQuantity,
        /**
         * Wed-demo: explicit order type ({@code "MARKET"} or {@code "LIMIT"}), decoupled from
         * {@link #limitPrice} presence. Lets the customer-FE blotter render "Market" for orders
         * the user placed as market even when {@code limitPrice} is set to a reference cap
         * supplied by the BFF (the price the venue may fill at-or-better).
         */
        String ordType
) {
    public static CreateOrderResponse from(Order o) {
        return from(o, null);
    }

    public static CreateOrderResponse from(Order o, String settlementStatus) {
        BigDecimal cumFilled = o.cumFilledQuantity() == null ? BigDecimal.ZERO : o.cumFilledQuantity();
        BigDecimal qty = o.quantity() == null ? BigDecimal.ZERO : o.quantity();
        // Subtract cum from total and clamp at zero. The clamp is defensive: the cluster guards
        // against cumFilled > quantity at admission time, but if a wire-format bug ever sneaks
        // an over-fill past, returning a negative leaves to a frontend would render as a
        // negative progress bar — better to flatten to zero and let the operator notice the
        // FILLED status vs. cumFilled discrepancy in a server log.
        BigDecimal leaves = qty.subtract(cumFilled);
        if (leaves.signum() < 0) {
            leaves = BigDecimal.ZERO;
        }
        return new CreateOrderResponse(
                o.id(), o.accountId(), o.clientIdempotencyKey(), o.shardId(), o.version(),
                o.status(),
                o.terminalReason() == null ? null : o.terminalReason().name(),
                o.side().name(), o.instrumentSymbol(), o.quantity(), o.limitPrice(),
                o.timeInForce(), o.receivedAt(), o.acceptedAt(), o.terminalAt(),
                o.ledgerBalanceId(),
                settlementStatus,
                cumFilled,
                leaves,
                o.ordType() == null ? "MARKET" : o.ordType()
        );
    }
}
