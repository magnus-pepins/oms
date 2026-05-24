package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.SessionID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Simple per-wire-session token bucket for FIX-in app messages. */
@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInAppMessageRateLimiter {

    private final int maxPerSecond;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public FixInAppMessageRateLimiter(OmsConfig omsConfig) {
        this.maxPerSecond = omsConfig.getFixIn().getMaxAppMessagesPerSecond();
    }

    public boolean tryAcquire(SessionID sessionId) {
        if (maxPerSecond <= 0) {
            return true;
        }
        long second = System.currentTimeMillis() / 1000L;
        String key = sessionId.toString() + ":" + second;
        Window w = windows.computeIfAbsent(key, k -> new Window(second));
        if (w.second != second) {
            windows.remove(key);
            w = windows.computeIfAbsent(key, k -> new Window(second));
        }
        return w.count.incrementAndGet() <= maxPerSecond;
    }

    private static final class Window {
        final long second;
        final AtomicInteger count = new AtomicInteger();

        Window(long second) {
            this.second = second;
        }
    }
}
