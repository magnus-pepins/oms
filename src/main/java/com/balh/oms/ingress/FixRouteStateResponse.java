package com.balh.oms.ingress;

import com.balh.oms.persistence.FixRouteStateRow;

import java.time.Instant;

/**
 * JSON for {@code GET /internal/v1/fix/route-state/{routeKey}}.
 */
public record FixRouteStateResponse(
        String routeKey,
        boolean sendEnabled,
        Instant updatedAt,
        String updatedBy,
        String note
) {
    public static FixRouteStateResponse from(FixRouteStateRow row) {
        return new FixRouteStateResponse(
                row.routeKey(), row.sendEnabled(), row.updatedAt(), row.updatedBy(), row.note());
    }
}
