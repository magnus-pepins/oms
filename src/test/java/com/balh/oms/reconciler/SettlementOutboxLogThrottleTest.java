package com.balh.oms.reconciler;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementOutboxLogThrottleTest {

    private static final String MSG =
            "customer balance not found in Ledger: indicator=inv-d2bc6c86-b79b-44df-ad5c-5f18fc43a67b-USD currency=USD";

    @Test
    void firstWarnInWindow_emitsFullDetail() {
        var throttle = new SettlementOutboxLogThrottle(60_000L);
        Instant t0 = Instant.parse("2026-05-20T10:00:00Z");

        assertThat(throttle.shouldEmitFullWarn("unfunded_balance", MSG, t0)).isTrue();
        assertThat(throttle.suppressedSuffix("unfunded_balance", MSG, t0)).isEmpty();
    }

    @Test
    void secondWarnSameKey_suppressesFullDetail() {
        var throttle = new SettlementOutboxLogThrottle(60_000L);
        Instant t0 = Instant.parse("2026-05-20T10:00:00Z");
        Instant t1 = t0.plusSeconds(1);

        assertThat(throttle.shouldEmitFullWarn("unfunded_balance", MSG, t0)).isTrue();
        assertThat(throttle.shouldEmitFullWarn("unfunded_balance", MSG, t1)).isFalse();
        assertThat(throttle.suppressedSuffix("unfunded_balance", MSG, t1))
                .contains("suppressed 1 identical");
    }

    @Test
    void afterWindow_eligibleForFullWarnAgain() {
        var throttle = new SettlementOutboxLogThrottle(5_000L);
        Instant t0 = Instant.parse("2026-05-20T10:00:00Z");
        Instant t1 = t0.plusSeconds(6);

        throttle.shouldEmitFullWarn("unfunded_balance", MSG, t0);
        throttle.shouldEmitFullWarn("unfunded_balance", MSG, t0.plusSeconds(1));

        assertThat(throttle.shouldEmitFullWarn("unfunded_balance", MSG, t1)).isTrue();
    }

    @Test
    void extractIndicator_parsesInvPattern() {
        assertThat(SettlementOutboxLogThrottle.extractIndicator(MSG))
                .isEqualTo("inv-d2bc6c86-b79b-44df-ad5c-5f18fc43a67b-USD");
    }
}
