package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Canonical JSON error body for internal HTTP APIs (slice 1).
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ApiErrorResponse(
        String rejectCode,
        String message,
        List<FieldViolation> fieldErrors
) {
    public record FieldViolation(String field, String message) {}
}
