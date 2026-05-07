package com.balh.oms.persistence;

import java.time.Instant;

/**
 * Row from {@code fix_route_state} (slice 4+).
 */
public record FixRouteStateRow(
        String routeKey,
        boolean sendEnabled,
        Instant updatedAt,
        String updatedBy,
        String note
) {}
