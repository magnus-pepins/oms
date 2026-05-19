package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Join view for advancing {@code executions.settlement_status} with order context.
 *
 * <p>{@code lastPrice} is the venue trade price for the fill (NULL only for
 * non-TRADE rows). It is required by the Ledger settlement outbox payload so
 * the worker can compute the cash leg (qty × price) without re-querying.
 *
 * <p>{@code orderId} is the parent order so the settlement fee leg can look up
 * the BFF-pinned commission from {@code order_fee_snapshots} (V40) instead of
 * recomputing from {@link StockCommissionCalculator}'s default schedule.
 */
public record SettlementExecutionRow(
        long executionId,
        UUID orderId,
        String settlementStatus,
        String execType,
        BigDecimal lastQuantity,
        BigDecimal lastPrice,
        UUID accountId,
        String instrumentSymbol,
        String side,
        BigDecimal sellPositionFromPendingBuy,
        BigDecimal sellPositionFromSettled) {}
