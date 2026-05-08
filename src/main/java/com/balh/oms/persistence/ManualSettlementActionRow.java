package com.balh.oms.persistence;

import java.time.Instant;

/** One row from {@code manual_settlement_actions}. */
public record ManualSettlementActionRow(
        long id,
        long executionId,
        String actionType,
        String requestedBy,
        String approvedBy,
        String payloadJson,
        Instant createdAt) {}
