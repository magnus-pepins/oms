package com.balh.oms.settlement;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Reduces per-{@code TRADE} execution {@code settlement_status} values to a single
 * order-level string for customer-facing surfaces (BFF → §12.9 chip).
 *
 * <p>Rules: if any trade leg is {@code failed}, the aggregate is {@code failed}. If every
 * leg is {@code settled}, the aggregate is {@code settled}. Otherwise the aggregate is the
 * highest-ranking non-{@code settled} leg ({@code executed} &lt; {@code matched} &lt;
 * {@code confirmed} &lt; {@code settling}).
 */
public final class OrderAggregateSettlementStatus {

    private OrderAggregateSettlementStatus() {}

    public static String summarize(List<String> tradeStatusesRaw) {
        if (tradeStatusesRaw == null || tradeStatusesRaw.isEmpty()) {
            return null;
        }
        List<String> normalized =
                tradeStatusesRaw.stream()
                        .filter(Objects::nonNull)
                        .map(s -> s.trim().toLowerCase(Locale.ROOT))
                        .filter(s -> !s.isEmpty())
                        .toList();
        if (normalized.isEmpty()) {
            return null;
        }
        if (normalized.stream().anyMatch("failed"::equals)) {
            return "failed";
        }
        if (normalized.stream().allMatch("settled"::equals)) {
            return "settled";
        }
        int bestRank = -1;
        String bestName = "executed";
        for (String s : normalized) {
            if ("settled".equals(s)) {
                continue;
            }
            int r = rank(s);
            if (r > bestRank) {
                bestRank = r;
                bestName = s;
            }
        }
        return bestRank < 0 ? "settled" : bestName;
    }

    private static int rank(String s) {
        return switch (s) {
            case "executed" -> 0;
            case "matched" -> 1;
            case "confirmed" -> 2;
            case "settling" -> 3;
            case "settled" -> 4;
            default -> 0;
        };
    }
}
