package com.balh.oms.reconciler;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rate-limits identical settlement-outbox WARN lines so demo fixtures with missing Ledger
 * balances do not flood pm2 logs between reconciler ticks.
 */
final class SettlementOutboxLogThrottle {

    private static final Pattern INDICATOR_IN_MESSAGE =
            Pattern.compile("indicator=([^\\s,]+)");

    private static final int MAX_TRACKED_KEYS = 512;

    private final long windowMs;
    private final ConcurrentHashMap<String, State> byKey = new ConcurrentHashMap<>();

    SettlementOutboxLogThrottle(long windowMs) {
        this.windowMs = Math.max(1_000L, windowMs);
    }

    /**
     * @return {@code true} when a full-detail WARN should be emitted now
     */
    boolean shouldEmitFullWarn(String reason, String message, Instant now) {
        String key = throttleKey(reason, message);
        long nowMs = now.toEpochMilli();
        State state = byKey.compute(key, (k, prev) -> {
            if (prev == null) {
                return new State(nowMs, 0);
            }
            if (nowMs - prev.windowStartEpochMs >= windowMs) {
                return new State(nowMs, 0);
            }
            return new State(prev.windowStartEpochMs, prev.suppressedInWindow + 1);
        });
        return state.suppressedInWindow == 0;
    }

    /**
     * Non-empty when the current tick suppressed a duplicate within the window (emit once after full WARN).
     */
    String suppressedSuffix(String reason, String message, Instant now) {
        String key = throttleKey(reason, message);
        State state = byKey.get(key);
        if (state == null || state.suppressedInWindow <= 0) {
            return "";
        }
        if (now.toEpochMilli() - state.windowStartEpochMs >= windowMs) {
            return "";
        }
        return " (suppressed " + state.suppressedInWindow + " identical warn(s) in last "
                + (windowMs / 1000)
                + "s)";
    }

    static String throttleKey(String reason, String message) {
        String indicator = extractIndicator(message);
        return reason + "|" + indicator;
    }

    static String extractIndicator(String message) {
        if (message == null || message.isBlank()) {
            return "_unknown";
        }
        Matcher m = INDICATOR_IN_MESSAGE.matcher(message);
        if (m.find()) {
            return m.group(1);
        }
        return message.length() > 120 ? message.substring(0, 120) : message;
    }

    void evictIfNeeded() {
        if (byKey.size() <= MAX_TRACKED_KEYS) {
            return;
        }
        byKey.clear();
    }

    private record State(long windowStartEpochMs, int suppressedInWindow) {}
}
