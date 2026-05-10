package com.balh.oms.ingress;

/** PATCH body for boolean {@code oms_runtime_flags} keys. */
public record BooleanRuntimeFlagPatchRequest(Boolean value, String updatedBy) {}
