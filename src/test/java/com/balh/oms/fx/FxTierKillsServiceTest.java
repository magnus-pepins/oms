package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pure-logic tests for {@link FxTierKillsService}.
 *
 * <p>Mirrors {@link FxMarkupOverridesServiceTest}: matcher behaviour over a
 * primed cache plus validation / four-eyes branches. The cache-load SQL
 * path is exercised by the Spring slice tests; here we test the contract
 * the publisher depends on staying in lockstep with what the operator
 * panel writes.
 */
class FxTierKillsServiceTest {

    private static final Instant T = Instant.parse("2026-05-20T12:00:00Z");
    private static final Instant FROM = T.minusSeconds(60);
    private static final Instant UNTIL = T.plusSeconds(60);

    private FxTierKillsService newService() {
        return new FxTierKillsService(
                mock(JdbcTemplate.class),
                Clock.fixed(T, ZoneOffset.UTC),
                new OmsConfig(),
                new SimpleMeterRegistry(),
                60_000L);
    }

    private FxTierKillsService newServiceWith(JdbcTemplate jdbc) {
        return new FxTierKillsService(
                jdbc,
                Clock.fixed(T, ZoneOffset.UTC),
                new OmsConfig(),
                new SimpleMeterRegistry(),
                60_000L);
    }

    private static FxTierKillsService.KillRow row(String pair, String tier) {
        return new FxTierKillsService.KillRow(pair, tier, FROM, UNTIL);
    }

    @Test
    void isKilled_falseWhenCacheEmpty() {
        FxTierKillsService svc = newService();
        svc.primeCache(List.of());
        assertThat(svc.isKilled("EURUSD", "elite", T)).isFalse();
    }

    @Test
    void isKilled_matchesExactPairTier() {
        FxTierKillsService svc = newService();
        svc.primeCache(List.of(row("EURUSD", "elite")));
        assertThat(svc.isKilled("EURUSD", "elite", T)).isTrue();
        assertThat(svc.isKilled("EURUSD", "basic", T)).isFalse();
        assertThat(svc.isKilled("GBPUSD", "elite", T)).isFalse();
    }

    @Test
    void isKilled_wildcardPairKillsThatTierAcrossEveryPair() {
        FxTierKillsService svc = newService();
        // pair=null → kill business across every pair (primary use case
        // for "halt this tier" during a vol spike).
        svc.primeCache(List.of(row(null, "business")));
        assertThat(svc.isKilled("EURUSD", "business", T)).isTrue();
        assertThat(svc.isKilled("GBPUSD", "business", T)).isTrue();
        assertThat(svc.isKilled("EURUSD", "elite", T)).isFalse();
    }

    @Test
    void isKilled_ignoresExpiredAndFutureWindows() {
        FxTierKillsService svc = newService();
        svc.primeCache(List.of(
                new FxTierKillsService.KillRow("EURUSD", "elite",
                        T.minusSeconds(120), T.minusSeconds(1)),  // expired
                new FxTierKillsService.KillRow("GBPUSD", "elite",
                        T.plusSeconds(60), T.plusSeconds(120))));  // future
        assertThat(svc.isKilled("EURUSD", "elite", T)).isFalse();
        assertThat(svc.isKilled("GBPUSD", "elite", T)).isFalse();
    }

    @Test
    void isKilled_normalisesCase() {
        FxTierKillsService svc = newService();
        svc.primeCache(List.of(row("EURUSD", "elite")));
        assertThat(svc.isKilled("eurusd", "ELITE", T)).isTrue();
    }

    @Test
    void shouldAutoApprove_scopedShortDurationApproves() {
        OmsConfig cfg = new OmsConfig();
        FxTierKillsService.CreateRequest req = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 5L * 60L * 1000L, "vol spike", "trader-a");
        assertThat(FxTierKillsService.shouldAutoApprove(req, cfg.getFx().getTierKills())).isTrue();
    }

    @Test
    void shouldAutoApprove_wildcardPairAlwaysRequiresFourEyes() {
        // A "kill business across every pair" affects every customer in
        // that tier — must require a second approver regardless of how
        // short the window is (plan A2 decision).
        OmsConfig cfg = new OmsConfig();
        FxTierKillsService.CreateRequest req = new FxTierKillsService.CreateRequest(
                null, "business", null, 60_000L, "kill", "trader-a");
        assertThat(FxTierKillsService.shouldAutoApprove(req, cfg.getFx().getTierKills())).isFalse();
    }

    @Test
    void shouldAutoApprove_durationAboveAutoApproveLimitRequiresFourEyes() {
        OmsConfig cfg = new OmsConfig();
        FxTierKillsService.CreateRequest req = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 60L * 60L * 1000L, "long", "trader-a");
        assertThat(FxTierKillsService.shouldAutoApprove(req, cfg.getFx().getTierKills())).isFalse();
    }

    @Test
    void shouldAutoApprove_disabledMeansEveryRowNeedsFourEyes() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFx().getTierKills().setAutoApproveEnabled(false);
        FxTierKillsService.CreateRequest req = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 1_000L, "tiny", "trader-a");
        assertThat(FxTierKillsService.shouldAutoApprove(req, cfg.getFx().getTierKills())).isFalse();
    }

    @Test
    void create_rejectsDurationBeyondHardCap() {
        FxTierKillsService svc = newService();
        FxTierKillsService.CreateRequest tooLong = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 365L * 24L * 60L * 60L * 1000L, "forever", "trader-a");
        assertThatThrownBy(() -> svc.create(tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds cap");
    }

    @Test
    void create_rejectsBlankTierReasonOrCreatedBy() {
        FxTierKillsService svc = newService();
        FxTierKillsService.CreateRequest blankTier = new FxTierKillsService.CreateRequest(
                "EURUSD", "  ", null, 60_000L, "ok", "trader-a");
        assertThatThrownBy(() -> svc.create(blankTier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tier");
        FxTierKillsService.CreateRequest blankReason = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 60_000L, "   ", "trader-a");
        assertThatThrownBy(() -> svc.create(blankReason))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
        FxTierKillsService.CreateRequest blankWho = new FxTierKillsService.CreateRequest(
                "EURUSD", "elite", null, 60_000L, "ok", "");
        assertThatThrownBy(() -> svc.create(blankWho))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("createdBy");
    }

    @Test
    void approve_rejectsSelfApproval() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxTierKillsService svc = newServiceWith(jdbc);
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
    void approve_rejectsAlreadyApprovedOrRevoked() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxTierKillsService svc = newServiceWith(jdbc);
        Map<String, Object> approved = new HashMap<>();
        approved.put("created_by", "trader-a");
        approved.put("approved_at", Timestamp.from(T));
        approved.put("revoked_at", null);
        when(jdbc.queryForMap(anyString(), eq(42L))).thenReturn(approved);
        assertThatThrownBy(() -> svc.approve(42L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already approved");

        Map<String, Object> revoked = new HashMap<>();
        revoked.put("created_by", "trader-a");
        revoked.put("approved_at", null);
        revoked.put("revoked_at", Timestamp.from(T));
        when(jdbc.queryForMap(anyString(), eq(43L))).thenReturn(revoked);
        assertThatThrownBy(() -> svc.approve(43L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("revoked");
    }

    @Test
    void approve_rejectsMissingRow() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxTierKillsService svc = newServiceWith(jdbc);
        when(jdbc.queryForMap(anyString(), eq(99L)))
                .thenThrow(new EmptyResultDataAccessException(1));
        assertThatThrownBy(() -> svc.approve(99L, "trader-b"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found");
    }
}
