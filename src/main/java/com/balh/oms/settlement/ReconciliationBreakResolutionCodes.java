package com.balh.oms.settlement;

import java.util.Set;

/** Ops resolution taxonomy for {@code reconciliation_breaks} (gap plan §5.2). */
public final class ReconciliationBreakResolutionCodes {

    public static final String BROKER_CORRECT = "broker_correct";
    public static final String OMS_CORRECT = "oms_correct";
    public static final String BROKER_CORRECTION_PENDING = "broker_correction_pending";
    public static final String WAIVED_OPS = "waived_ops";

    private static final Set<String> RESOLVE_CODES =
            Set.of(BROKER_CORRECT, OMS_CORRECT, BROKER_CORRECTION_PENDING);

    private ReconciliationBreakResolutionCodes() {}

    public static boolean isValidResolveCode(String code) {
        return code != null && RESOLVE_CODES.contains(code.trim());
    }
}
