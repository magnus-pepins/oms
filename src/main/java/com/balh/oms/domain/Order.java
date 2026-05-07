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
        String accountIdHash
) {}
