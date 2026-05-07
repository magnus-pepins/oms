package com.balh.oms.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderPartiallyFilledEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        BigDecimal cumulativeFilledQuantity,
        BigDecimal lastQuantity,
        BigDecimal lastPrice,
        String venueId,
        String venueExecRef,
        Instant occurredAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderPartiallyFilled";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
