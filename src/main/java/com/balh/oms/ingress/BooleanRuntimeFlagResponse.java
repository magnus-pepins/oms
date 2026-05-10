package com.balh.oms.ingress;

import java.time.Instant;

/** JSON for boolean {@code oms_runtime_flags} rows (generic shape). */
public record BooleanRuntimeFlagResponse(boolean value, Instant updatedAt) {
    public static BooleanRuntimeFlagResponse absent() {
        return new BooleanRuntimeFlagResponse(false, null);
    }
}
