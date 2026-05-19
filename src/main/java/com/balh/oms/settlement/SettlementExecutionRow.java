package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Join view for advancing {@code executions.settlement_status} with order context.
 *
 * <p>{@code lastPrice} is the venue trade price for the fill (NULL only for
 * non-TRADE rows). It is required by the Ledger settlement outbox payload so
 * the worker can compute the cash leg (qty × price) without re-querying.
 */
public record SettlementExecutionRow(
        long executionId,
        String settlementStatus,
        String execType,
        BigDecimal lastQuantity,
        BigDecimal lastPrice,
        UUID accountId,
        String instrumentSymbol,
        String side,
        BigDecimal sellPositionFromPendingBuy,
        BigDecimal sellPositionFromSettled) {}
