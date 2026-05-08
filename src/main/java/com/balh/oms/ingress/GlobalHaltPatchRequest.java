package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

/**
 * Body for {@code PATCH /internal/v1/runtime-flags/global_halt}.
 *
 * @param globalHalt target halt flag
 * @param updatedBy optional actor label for logs (defaults to {@code internal-api} in controller)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GlobalHaltPatchRequest(@NotNull Boolean globalHalt, String updatedBy) {}
