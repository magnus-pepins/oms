package com.balh.oms.chronicle;

import java.util.Locale;

/**
 * How {@code control_outbox} rows become Chronicle excerpts (see {@code plans/oms-ingress-control-fix-topology.md} P2).
 */
public final class ControlChronicleAppendMode {

    private ControlChronicleAppendMode() {}

    /** Scheduled {@link com.balh.oms.reconciler.OutboxReconciler} drains Postgres and appends (default). */
    public static final String RECONCILER = "reconciler";

    /**
     * Ingress JVM appends and marks the row immediately after the accept transaction commits; reconciler bean is
     * disabled so the journal is not written twice.
     */
    public static final String INGRESS_AFTER_COMMIT = "ingress-after-commit";

    public static String normalize(String raw) {
        if (raw == null || raw.isBlank()) {
            return RECONCILER;
        }
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    public static void validate(String normalized) {
        if (!RECONCILER.equals(normalized) && !INGRESS_AFTER_COMMIT.equals(normalized)) {
            throw new IllegalArgumentException(
                    "oms.control.chronicle-append-mode must be '" + RECONCILER + "' or '" + INGRESS_AFTER_COMMIT + "', got: "
                            + normalized);
        }
    }
}
