package com.balh.oms.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderAcceptedEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        String side,
        String instrumentSymbol,
        BigDecimal quantity,
        BigDecimal limitPrice,
        String timeInForce,
        Instant acceptedAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderAccepted";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
