package com.balh.oms.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Immutable in-memory view of a row in {@code orders}.
 *
 * <p>{@code version} is the per-row CAS counter and the canonical {@code event_seq}
 * for downstream consumers. Mutations go through
 * {@code OrdersRepository.updateWithCas(...)}.
 */
public record Order(
        UUID id,
        UUID accountId,
        String clientIdempotencyKey,
        int shardId,
        int version,
        OrderStatus status,
        RejectCode terminalReason,
        Side side,
        String instrumentSymbol,
        BigDecimal quantity,
        BigDecimal limitPrice,
        String timeInForce,
        Instant receivedAt,
        Instant acceptedAt,
        Instant terminalAt,
        String accountIdHash,
        /** Ledger {@code balance_id} when supplied on ingress; used for BUY buying-power checks. */
        String ledgerBalanceId,
        /** Cumulative filled quantity from venue execution reports (slice 3). */
        BigDecimal cumFilledQuantity,
        /**
         * Order type, {@code "MARKET"} or {@code "LIMIT"}. Decoupled from {@link #limitPrice}
         * presence as of the Wed-demo MARKET-with-reference-price work: a MARKET order may now
         * carry a {@code limitPrice} that acts as a fill cap (used by OMS to size the BUY
         * inflight hold and by FIX egress to emit a non-zero {@code Price} tag).
         */
        String ordType
) {
    /**
     * Back-compat constructor used by older callers / tests that pre-date the Wed-demo
     * {@code ordType} field. Infers from {@code limitPrice}: {@code MARKET} when {@code null},
     * {@code LIMIT} otherwise. Mirrors {@link com.balh.oms.ingress.CreateOrderRequest#resolvedOrderType()}.
     */
    public Order(
            UUID id,
            UUID accountId,
            String clientIdempotencyKey,
            int shardId,
            int version,
            OrderStatus status,
            RejectCode terminalReason,
            Side side,
            String instrumentSymbol,
            BigDecimal quantity,
            BigDecimal limitPrice,
            String timeInForce,
            Instant receivedAt,
            Instant acceptedAt,
            Instant terminalAt,
            String accountIdHash,
            String ledgerBalanceId,
            BigDecimal cumFilledQuantity) {
        this(id, accountId, clientIdempotencyKey, shardId, version, status, terminalReason, side,
                instrumentSymbol, quantity, limitPrice, timeInForce, receivedAt, acceptedAt,
                terminalAt, accountIdHash, ledgerBalanceId, cumFilledQuantity,
                limitPrice == null ? "MARKET" : "LIMIT");
    }
}
