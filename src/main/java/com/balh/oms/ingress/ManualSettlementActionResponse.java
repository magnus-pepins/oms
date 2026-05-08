package com.balh.oms.ingress;

import java.time.Instant;

/** JSON row for manual settlement actions (list/detail). */
public record ManualSettlementActionResponse(
        long id,
        long executionId,
        String actionType,
        String requestedBy,
        String approvedBy,
        String payloadJson,
        Instant createdAt) {}
