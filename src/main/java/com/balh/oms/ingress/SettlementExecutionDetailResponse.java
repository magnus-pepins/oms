package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/** JSON for {@code GET /internal/v1/settlement/executions/{id}}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SettlementExecutionDetailResponse(
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
