package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * One execution row joined to order, including raw venue envelope JSON (detail read).
 *
 * <p>{@code tradeDate} and {@code expectedSettlementDate} are populated by
 * {@link com.balh.oms.settlement.SettlementDateCalculator} on TRADE inserts (gap plan §5.3
 * Slice 1 / 2b — V58 column + holiday-aware compute). Both are NULL on CANCEL / REJECT /
 * REPLACE executions, on TRADE rows written before V58, and (for
 * {@code expectedSettlementDate} only) on pre-V58 rows that V62 backfilled
 * {@code tradeDate} on without inventing a historical cycle.
 */
public record SettlementExecutionDetailRow(
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
        String instrumentSymbol,
        LocalDate tradeDate,
        LocalDate expectedSettlementDate,
        String rawEnvelopeJson) {}
