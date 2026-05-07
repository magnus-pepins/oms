package com.balh.oms.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderFilledEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        BigDecimal filledQuantity,
        BigDecimal averageFillPrice,
        String venueId,
        String venueExecRef,
        Instant occurredAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderFilled";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
