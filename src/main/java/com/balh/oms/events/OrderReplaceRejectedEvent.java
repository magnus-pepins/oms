package com.balh.oms.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when the broker declined our 35=G replace (35=9 OrderCancelReject against a replace).
 * Same shape and semantics as {@link OrderCancelRejectedEvent} — the order is unchanged, the row
 * is informational — but a distinct type so downstream surfaces can produce the right toast
 * copy ("modify rejected" vs. "cancel rejected").
 */
public record OrderReplaceRejectedEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        String currentStatus,
        String venueId,
        String venueExecRef,
        Instant occurredAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderReplaceRejected";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
