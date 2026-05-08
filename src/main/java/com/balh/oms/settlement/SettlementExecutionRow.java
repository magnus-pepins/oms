package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Join view for advancing {@code executions.settlement_status} with order context.
 */
public record SettlementExecutionRow(
        long executionId,
        String settlementStatus,
        String execType,
        BigDecimal lastQuantity,
        UUID accountId,
        String instrumentSymbol,
        String side,
        BigDecimal sellPositionFromPendingBuy,
        BigDecimal sellPositionFromSettled) {}
