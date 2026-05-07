package com.balh.oms.events;

import java.time.Instant;
import java.util.UUID;

public record OrderCancelledEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        String venueId,
        String venueExecRef,
        Instant occurredAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderCancelled";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
