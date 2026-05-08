package com.balh.oms.settlement;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Valid forward steps for {@code execution_settlement_status} (§12.3).
 */
public final class SettlementStateMachine {

    private static final Map<String, String> NEXT = Map.of(
            "executed", "matched",
            "matched", "confirmed",
            "confirmed", "settling",
            "settling", "settled");

    private SettlementStateMachine() {}

    public static Optional<String> next(String current) {
        if (current == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(NEXT.get(current.trim().toLowerCase(Locale.ROOT)));
    }

    public static boolean isTerminal(String status) {
        if (status == null) {
            return false;
        }
        String s = status.trim().toLowerCase(Locale.ROOT);
        return "settled".equals(s) || "failed".equals(s);
    }
}
