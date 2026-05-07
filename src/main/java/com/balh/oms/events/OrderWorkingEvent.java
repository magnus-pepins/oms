package com.balh.oms.events;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.domain.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Emitted after a successful CAS transition of {@code orders} to {@code WORKING}.
 *
 * <p>{@code eventSeq} is the new {@code orders.version} after the update.
 */
public record OrderWorkingEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        String side,
        String instrumentSymbol,
        BigDecimal quantity,
        BigDecimal limitPrice,
        String timeInForce,
        Instant workingAt
) implements DomainEvent {

    public static OrderWorkingEvent afterWorking(PendingControlEvent event, Order order, int newSeq) {
        return new OrderWorkingEvent(
                order.id(),
                newSeq,
                event.shardId(),
                event.accountIdHash(),
                order.side().name(),
                order.instrumentSymbol(),
                order.quantity(),
                order.limitPrice(),
                order.timeInForce(),
                Instant.now());
    }

    @Override
    public String type() {
        return "OrderWorking";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
