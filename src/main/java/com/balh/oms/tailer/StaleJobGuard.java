package com.balh.oms.tailer;

import com.balh.oms.config.OmsConfig;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Drops control jobs older than {@code oms.control.max-job-age-ms}.
 *
 * <p>Mirrors the §5.9 contract: if a control event sat in the journal so long
 * that we can't trust risk decisions made against stale market context, mark
 * the job as STALE and reject the order with {@code RISK_STALE_QUEUE}.
 */
@Component
public class StaleJobGuard {

    private final long maxAgeMs;

    public StaleJobGuard(OmsConfig config) {
        this.maxAgeMs = config.getControl().getMaxJobAgeMs();
    }

    public boolean isStale(Instant orderTimestamp) {
        return Duration.between(orderTimestamp, Instant.now()).toMillis() > maxAgeMs;
    }

    public long maxAgeMs() {
        return maxAgeMs;
    }
}
