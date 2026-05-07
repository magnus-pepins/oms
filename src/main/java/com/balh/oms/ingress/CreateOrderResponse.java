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
        String ledgerBalanceId
) {
    public static CreateOrderResponse from(Order o) {
        return new CreateOrderResponse(
                o.id(), o.accountId(), o.clientIdempotencyKey(), o.shardId(), o.version(),
                o.status(),
                o.terminalReason() == null ? null : o.terminalReason().name(),
                o.side().name(), o.instrumentSymbol(), o.quantity(), o.limitPrice(),
                o.timeInForce(), o.receivedAt(), o.acceptedAt(), o.terminalAt(),
                o.ledgerBalanceId()
        );
    }
}
