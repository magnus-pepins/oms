package com.balh.oms.persistence;

import java.time.Instant;
import java.util.UUID;

/** Read model for one {@code control_decisions} row. */
public record ControlDecisionRow(
        long id,
        UUID orderId,
        int orderVersionBefore,
        String outcome,
        String rejectCode,
        String stage,
        String detail,
        Instant decidedAt) {}
