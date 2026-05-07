package com.balh.oms.events;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.domain.RejectCode;

import java.time.Instant;
import java.util.UUID;

/**
 * Emitted after a successful CAS transition of {@code orders} to {@code REJECTED}.
 *
 * <p>{@code eventSeq} is the new {@code orders.version} after the update (the prior
 * tail event's {@code orderVersion} + 1 when the CAS row wins).
 */
public record OrderRejectedEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        String rejectCode,
        Instant terminalAt
) implements DomainEvent {

    public static OrderRejectedEvent afterReject(PendingControlEvent event, RejectCode reason, int newVersion) {
        return new OrderRejectedEvent(
                event.orderId(),
                newVersion,
                event.shardId(),
                event.accountIdHash(),
                reason.name(),
                Instant.now());
    }

    @Override
    public String type() {
        return "OrderRejected";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
