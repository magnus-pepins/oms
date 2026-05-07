package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Simple token bucket for FIX outbound NOS pacing (slice 4+). When
 * {@code oms.fix.outbound-tokens-per-second} is {@code <= 0}, {@link #tryAcquire()} always succeeds.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixOutboundTokenBucket {

    private final OmsConfig omsConfig;
    private double tokens;
    private long lastRefillNanos;

    public FixOutboundTokenBucket(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
        this.tokens = Math.max(0, omsConfig.getFix().getOutboundTokenBurst());
        this.lastRefillNanos = System.nanoTime();
    }

    /**
     * @return {@code false} if throttled (caller should re-queue the order id)
     */
    public synchronized boolean tryAcquire() {
        double rate = omsConfig.getFix().getOutboundTokensPerSecond();
        if (rate <= 0) {
            return true;
        }
        int burst = Math.max(1, omsConfig.getFix().getOutboundTokenBurst());
        refill(rate, burst);
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }

    private void refill(double ratePerSec, int burst) {
        long now = System.nanoTime();
        double elapsedSec = (now - lastRefillNanos) / 1_000_000_000.0;
        if (elapsedSec > 0) {
            tokens = Math.min(burst, tokens + elapsedSec * ratePerSec);
            lastRefillNanos = now;
        }
    }
}
