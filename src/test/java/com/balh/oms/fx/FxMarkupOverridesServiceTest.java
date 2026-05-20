package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                new OmsConfig(),
                new SimpleMeterRegistry(),
                60_000L);
    }

    private FxMarkupOverridesService newServiceWith(JdbcTemplate jdbc) {
        return new FxMarkupOverridesService(
                jdbc,
                Clock.fixed(T, ZoneOffset.UTC),
                new OmsConfig(),
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
    void shouldAutoApprove_smallBpsShortDurationApproves() {
        OmsConfig cfg = new OmsConfig();
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 5L * 60L * 1000L, "vol spike", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(req, cfg.getFx().getMarkupOverrides()))
                .isTrue();
    }

    @Test
    void shouldAutoApprove_bpsAboveAutoApproveLimitRequiresFourEyes() {
        OmsConfig cfg = new OmsConfig();
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("20.00"),
                null, 5L * 60L * 1000L, "vol spike", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(req, cfg.getFx().getMarkupOverrides()))
                .isFalse();
    }

    @Test
    void shouldAutoApprove_durationAboveAutoApproveLimitRequiresFourEyes() {
        OmsConfig cfg = new OmsConfig();
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 60L * 60L * 1000L, "long window", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(req, cfg.getFx().getMarkupOverrides()))
                .isFalse();
    }

    @Test
    void shouldAutoApprove_disabledMeansEveryRowNeedsFourEyes() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFx().getMarkupOverrides().setAutoApproveEnabled(false);
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("0.01"),
                null, 1_000L, "tiny", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(req, cfg.getFx().getMarkupOverrides()))
                .isFalse();
    }

    @Test
    void shouldAutoApprove_negativeBpsRespectsAbsoluteLimit() {
        OmsConfig cfg = new OmsConfig();
        FxMarkupOverridesService.CreateRequest tight = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("-3.00"),
                null, 60_000L, "marketing window", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(tight, cfg.getFx().getMarkupOverrides()))
                .isTrue();

        FxMarkupOverridesService.CreateRequest wide = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("-20.00"),
                null, 60_000L, "promo", "trader-a");
        assertThat(FxMarkupOverridesService.shouldAutoApprove(wide, cfg.getFx().getMarkupOverrides()))
                .isFalse();
    }

    @Test
    void create_rejectsBpsBeyondHardCap() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.CreateRequest tooBig = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("99999.00"),
                null, 60_000L, "too much", "trader-a");
        assertThatThrownBy(() -> svc.create(tooBig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds cap");
    }

    @Test
    void create_rejectsDurationBeyondHardCap() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.CreateRequest tooLong = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 365L * 24L * 60L * 60L * 1000L, "forever", "trader-a");
        assertThatThrownBy(() -> svc.create(tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds cap");
    }

    @Test
    void create_rejectsBlankReasonOrCreatedBy() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.CreateRequest blankReason = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 60_000L, "   ", "trader-a");
        assertThatThrownBy(() -> svc.create(blankReason))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
        FxMarkupOverridesService.CreateRequest blankWho = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 60_000L, "ok", "");
        assertThatThrownBy(() -> svc.create(blankWho))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("createdBy");
    }

    @Test
    void create_rejectsInvalidSide() {
        FxMarkupOverridesService svc = newService();
        FxMarkupOverridesService.CreateRequest bad = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BOTH", "elite", new BigDecimal("3.00"),
                null, 60_000L, "no", "trader-a");
        assertThatThrownBy(() -> svc.create(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("side");
    }

    @Test
    void approve_rejectsSelfApproval() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        Map<String, Object> row = new HashMap<>();
        row.put("created_by", "trader-a");
        row.put("approved_at", null);
        row.put("revoked_at", null);
        when(jdbc.queryForMap(anyString(), eq(42L))).thenReturn(row);

        assertThatThrownBy(() -> svc.approve(42L, "trader-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("four-eyes");
    }

    @Test
    void approve_rejectsAlreadyApproved() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        Map<String, Object> row = new HashMap<>();
        row.put("created_by", "trader-a");
        row.put("approved_at", Timestamp.from(T));
        row.put("revoked_at", null);
        when(jdbc.queryForMap(anyString(), eq(42L))).thenReturn(row);

        assertThatThrownBy(() -> svc.approve(42L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already approved");
    }

    @Test
    void approve_rejectsRevoked() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        Map<String, Object> row = new HashMap<>();
        row.put("created_by", "trader-a");
        row.put("approved_at", null);
        row.put("revoked_at", Timestamp.from(T));
        when(jdbc.queryForMap(anyString(), eq(42L))).thenReturn(row);

        assertThatThrownBy(() -> svc.approve(42L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("revoked");
    }

    @Test
    void approve_rejectsMissingRow() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        when(jdbc.queryForMap(anyString(), eq(99L)))
                .thenThrow(new EmptyResultDataAccessException(1));

        assertThatThrownBy(() -> svc.approve(99L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void approve_writesApprovalRowAndRefreshes() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        Map<String, Object> row = new HashMap<>();
        row.put("created_by", "trader-a");
        row.put("approved_at", null);
        row.put("revoked_at", null);
        when(jdbc.queryForMap(anyString(), eq(7L))).thenReturn(row);
        when(jdbc.update(anyString(), any(), any(), eq(7L))).thenReturn(1);

        svc.approve(7L, "trader-b");
        // verifies both that the UPDATE returned 1 (otherwise IllegalStateException would have been thrown)
        // and that refreshNow ran without complaint
    }

    @Test
    void revoke_rejectsAlreadyRevokedOrMissingRow() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        when(jdbc.update(anyString(), any(), any(), eq(7L))).thenReturn(0);

        assertThatThrownBy(() -> svc.revoke(7L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already revoked");
    }

    @Test
    void writePaths_firePropagatorWithActionAndId() {
        // Pins the contract that FxMarkupOverridesNatsInvalidationBus relies on:
        // every successful create/approve/revoke fires localChanged so remote
        // JVMs invalidate their cache without waiting for the scheduled refresh.
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        OverridesChangePropagator propagator = mock(OverridesChangePropagator.class);
        svc.setChangePropagator(propagator);

        // create — small bps, short duration → auto-approve, INSERT returns id 11
        when(jdbc.queryForObject(anyString(), eq(Long.class),
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(11L);
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                "EURUSD", "BID", "elite", new BigDecimal("3.00"),
                null, 60_000L, "vol spike", "trader-a");
        svc.create(req);

        // approve — different operator passes four-eyes, UPDATE returns 1
        Map<String, Object> pendingRow = new HashMap<>();
        pendingRow.put("created_by", "trader-a");
        pendingRow.put("approved_at", null);
        pendingRow.put("revoked_at", null);
        when(jdbc.queryForMap(anyString(), eq(11L))).thenReturn(pendingRow);
        when(jdbc.update(anyString(), any(), any(), eq(11L))).thenReturn(1);
        svc.approve(11L, "trader-b");

        // revoke — UPDATE returns 1
        when(jdbc.update(anyString(), any(), any(), eq(11L))).thenReturn(1);
        svc.revoke(11L, "trader-b");

        org.mockito.Mockito.verify(propagator).localChanged("create", 11L);
        org.mockito.Mockito.verify(propagator).localChanged("approve", 11L);
        org.mockito.Mockito.verify(propagator).localChanged("revoke", 11L);
    }

    @Test
    void setChangePropagator_nullResetsToNoop() {
        // Operator concern: PreDestroy on the bus calls setChangePropagator(NOOP)
        // so a write that arrives mid-shutdown does not NPE. Pass null and
        // confirm a subsequent write still completes.
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxMarkupOverridesService svc = newServiceWith(jdbc);
        svc.setChangePropagator(mock(OverridesChangePropagator.class));
        svc.setChangePropagator(null);
        when(jdbc.update(anyString(), any(), any(), eq(8L))).thenReturn(1);
        // should not throw
        svc.revoke(8L, "trader-b");
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
