package com.balh.oms.fx;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Pure-logic tests for {@link FxMarkupOverridesService}. The cache-load SQL
 * path is exercised by the Spring slice tests; here we only verify the
 * matcher behaviour over a primed cache, since that is the contract both
 * {@link OmsFxCustomerQuotePublisher#lookupMarkup} and
 * {@link FxQuoteService#lookupMarkupBps} rely on staying in lockstep.
 */
class FxMarkupOverridesServiceTest {

    private static final Instant T = Instant.parse("2026-05-20T12:00:00Z");
    private static final Instant FROM = T.minusSeconds(60);
    private static final Instant UNTIL = T.plusSeconds(60);

    private FxMarkupOverridesService newService() {
        return new FxMarkupOverridesService(
                mock(JdbcTemplate.class),
                Clock.fixed(T, ZoneOffset.UTC),
                new SimpleMeterRegistry(),
                60_000L);
    }

    private static FxMarkupOverridesService.OverrideRow row(
            String pair, String side, String tier, String additiveBps) {
        return new FxMarkupOverridesService.OverrideRow(
                pair, side, tier, new BigDecimal(additiveBps), FROM, UNTIL);
    }

    @Test
    void additiveBpsFor_returnsZeroWhenCacheEmpty() {
        FxMarkupOverridesService svc = newService();
        svc.primeCache(List.of());
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("0");
    }

    @Test
    void additiveBpsFor_matchesExactRow() {
        FxMarkupOverridesService svc = newService();
        svc.primeCache(List.of(row("EURUSD", "BID", "elite", "5.00")));
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("5.00");
    }

    @Test
    void additiveBpsFor_wildcardScopeApplies() {
        FxMarkupOverridesService svc = newService();
        // null pair / null side / null tier → applies to anything
        svc.primeCache(List.of(row(null, null, null, "2.50")));
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("2.50");
        assertThat(svc.additiveBpsFor("GBPUSD", "ASK", "basic", T)).isEqualByComparingTo("2.50");
    }

    @Test
    void additiveBpsFor_partialWildcardScopeApplies() {
        FxMarkupOverridesService svc = newService();
        // tier=elite, any pair, any side
        svc.primeCache(List.of(row(null, null, "elite", "3.00")));
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("3.00");
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "basic", T)).isEqualByComparingTo("0");
    }

    @Test
    void additiveBpsFor_sumsMultipleMatchingRows() {
        FxMarkupOverridesService svc = newService();
        svc.primeCache(List.of(
                row("EURUSD", null, null, "4.00"),
                row(null, "BID", null, "1.50"),
                row(null, null, "elite", "2.00"),
                row("GBPUSD", null, null, "10.00")
        ));
        // EURUSD BID elite matches first three; GBPUSD row excluded
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("7.50");
    }

    @Test
    void additiveBpsFor_ignoresExpiredRows() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.OverrideRow expired = new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite",
                new BigDecimal("100.00"),
                T.minusSeconds(120),
                T.minusSeconds(1));
        svc.primeCache(List.of(expired));
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("0");
    }

    @Test
    void additiveBpsFor_ignoresFutureRows() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.OverrideRow future = new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite",
                new BigDecimal("100.00"),
                T.plusSeconds(60),
                T.plusSeconds(120));
        svc.primeCache(List.of(future));
        assertThat(svc.additiveBpsFor("EURUSD", "BID", "elite", T)).isEqualByComparingTo("0");
    }

    @Test
    void additiveBpsFor_normalisesCaseOnInput() {
        FxMarkupOverridesService svc = newService();
        svc.primeCache(List.of(row("EURUSD", "BID", "elite", "5.00")));
        // callers may pass any casing; matching is canonical
        assertThat(svc.additiveBpsFor("eurusd", "bid", "ELITE", T)).isEqualByComparingTo("5.00");
    }

    @Test
    void status_countsActiveAndReportsNextExpiry() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.OverrideRow active1 = new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("5.00"),
                T.minusSeconds(60), T.plusSeconds(30));
        FxMarkupOverridesService.OverrideRow active2 = new FxMarkupOverridesService.OverrideRow(
                "GBPUSD", null, null, new BigDecimal("2.00"),
                T.minusSeconds(60), T.plusSeconds(120));
        FxMarkupOverridesService.OverrideRow future = new FxMarkupOverridesService.OverrideRow(
                "USDJPY", null, null, new BigDecimal("1.00"),
                T.plusSeconds(30), T.plusSeconds(60));
        svc.primeCache(List.of(active1, active2, future));
        FxMarkupOverridesService.OverridesStatus s = svc.status();
        assertThat(s.cachedRowCount()).isEqualTo(3);
        assertThat(s.activeCount()).isEqualTo(2);
        assertThat(s.nextExpiryEpochMs()).isEqualTo(active1.validUntil().toEpochMilli());
    }
}
