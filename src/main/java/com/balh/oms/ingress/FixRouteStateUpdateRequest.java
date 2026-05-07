package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 * PATCH body for {@code /internal/v1/fix/route-state/{routeKey}}.
 */
public record FixRouteStateUpdateRequest(
        @JsonProperty("sendEnabled") @NotNull Boolean sendEnabled,
        String note,
        String updatedBy
) {}
