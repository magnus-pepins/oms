package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** One execution row joined to order, including raw venue envelope JSON (detail read). */
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
        String rawEnvelopeJson) {}
