package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.UUID;

/**
 * JSON row for {@code GET /internal/v1/control-decisions}.
 *
 * @param rejectCode enum name when {@code outcome=REJECT}; otherwise null
 * @param detail JSON object as a string (may be "{}"); never null for clients
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ControlDecisionResponse(
        long id,
        UUID orderId,
        int orderVersionBefore,
        String outcome,
        String rejectCode,
        String stage,
        String detail,
        Instant decidedAt) {}
