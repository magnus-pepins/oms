package com.balh.oms.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted when the broker declined our 35=F cancel (35=9 OrderCancelReject). No state change to
 * the order — it stays in whatever status / qty / cumQty it had before — but the projector still
 * lands an audit row in {@code executions} ({@code exec_type=CANCEL_REJECT}) and emits this
 * envelope so the BFF can surface a toast / log without polling.
 *
 * <p>The {@code eventSeq} on this envelope deliberately mirrors the current
 * {@code orders.version} (not version + 1), because the orders row is not mutated by a reject.
 * Downstream consumers that sequence on {@code (orderId, eventSeq)} pair this envelope with the
 * already-committed working/partially-filled status; the reject is informational, not a state
 * transition.
 */
public record OrderCancelRejectedEvent(
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
        return "OrderCancelRejected";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
