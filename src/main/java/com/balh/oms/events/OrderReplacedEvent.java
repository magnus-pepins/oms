package com.balh.oms.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Emitted after a successful CAS transition of {@code orders.quantity} / {@code orders.limit_price}
 * to the broker-authoritative replacement values (venue ACK of a 35=G we sent). Carries the new
 * total quantity and the new limit price so downstream BFF consumers can mirror them onto the
 * customer-facing row ({@code customer_orders.qty}, {@code customer_orders.limit_price}) without a
 * second OMS read.
 *
 * <p>{@code eventSeq} is the new {@code orders.version} after the CAS — same convention as the
 * {@code OrderCancelled} / {@code OrderFilled} envelopes.
 *
 * <p>{@code newLimitPrice} is {@code null} only when the replace targeted a market order (no limit
 * price), which is exotic; the limit-order replace path always carries the new price.
 *
 * <p>{@code newStatus} echoes the cluster's post-replace status string (one of {@code WORKING},
 * {@code PARTIALLY_FILLED}, {@code FILLED}). It matches the {@code orders.status} row after the
 * CAS; downstream consumers that key off OMS-native status names can use this directly instead of
 * re-reading the row.
 */
public record OrderReplacedEvent(
        UUID orderId,
        int eventSeq,
        int shardId,
        String accountIdHash,
        BigDecimal newQuantity,
        BigDecimal newLimitPrice,
        String newStatus,
        String venueId,
        String venueExecRef,
        Instant occurredAt
) implements DomainEvent {

    @Override
    public String type() {
        return "OrderReplaced";
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
