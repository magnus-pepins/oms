package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** One row from {@link SettlementExecutionsRepository#findByFilters}. */
public record SettlementExecutionRow(
        long id,
        UUID orderId,
        UUID accountId,
        String venueId,
        Instant venueTs,
        String venueExecRef,
        BigDecimal lastQuantity,
        BigDecimal lastPrice,
        BigDecimal leavesQuantity,
        BigDecimal cumQuantityAfter,
        String execType,
        String settlementStatus,
        Instant createdAt,
        String orderStatus,
        String side,
        String instrumentSymbol) {}
