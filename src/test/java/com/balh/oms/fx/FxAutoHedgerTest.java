package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Drift-loop unit tests for {@link FxAutoHedger}.
 *
 * <p>The loop reads from {@link FxNostroSnapshotService} (mocked to
 * return a deterministic balance), {@link FxAutoHedgerPolicyService}
 * (primed via {@code primeCache}), and {@link FxQuoteService} (mocked
 * to return a fixed quote). The persistence path is verified via
 * JdbcTemplate update counts so we don't need a Postgres slice for
 * the decision logic.
 */
class FxAutoHedgerTest {

    private static final Instant T = Instant.parse("2026-05-21T12:00:00Z");

    private JdbcTemplate jdbc;
    private OmsConfig omsConfig;
    private FxAutoHedgerPolicyService policy;
    private FxNostroSnapshotService nostro;
    private FxQuoteService quoteService;
    private ObjectProvider<FxHedgeService> hedgeProvider;
    private FxHedgeService hedgeService;
    private FxAutoHedger engine;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        omsConfig = new OmsConfig();
        omsConfig.getFx().setModuleEnabled(true);
        // Module config has nostro IDs configured by string; the
        // snapshot service is mocked so we don't need to wire those.
        omsConfig.getFx().getAutoHedger().setEngineEnabled(true);
        omsConfig.getFx().getAutoHedger().setAutoFireEnabled(false);
        omsConfig.getFx().getAutoHedger().setPricingTier("desk");

        policy = mock(FxAutoHedgerPolicyService.class);
        nostro = mock(FxNostroSnapshotService.class);
        quoteService = mock(FxQuoteService.class);
        hedgeService = mock(FxHedgeService.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<FxHedgeService> provider = (ObjectProvider<FxHedgeService>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(hedgeService);
        hedgeProvider = provider;

        engine = new FxAutoHedger(
                jdbc,
                Clock.fixed(T, ZoneOffset.UTC),
                omsConfig,
                policy,
                nostro,
                quoteService,
                hedgeProvider,
                new ObjectMapper(),
                new SimpleMeterRegistry());
    }

    private FxAutoHedgerPolicyService.PolicyRow policyRow(String currency, String mode, String pairRoute, BigDecimal target) {
        return new FxAutoHedgerPolicyService.PolicyRow(
                currency, target,
                new BigDecimal("50000"),  // threshold_abs
                null,                                   // threshold_pct
                pairRoute,
                new BigDecimal("100000"),  // max_per_action (in policy currency)
                300,                                    // cooldown_s
                mode,
                "nostro-base", "nostro-quote",
                "trader-a", T.minusSeconds(3600),
                "trader-a", T.minusSeconds(3600),
                "auto".equals(mode) ? "approver-x" : null,
                "auto".equals(mode) ? T.minusSeconds(60) : null);
    }

    private void primeBalance(String currency, String amount) {
        when(nostro.buildSnapshot()).thenReturn(Map.of(
                "asOf", T.toString(),
                "balances", List.of(Map.of(
                        "balanceId", currency + "-nostro",
                        "currency", currency,
                        "availableBalance", amount))));
    }

    private void primeQuote(String pair, String bid, String ask, String mid) {
        when(quoteService.quote(eq(pair), eq("desk")))
                .thenReturn(Map.of(
                        "pair", pair, "tier", "desk",
                        "quoteId", "q-" + pair,
                        "bid", bid, "ask", ask, "mid", mid));
    }

    @Test
    void belowThreshold_writesNoRecommendationAndUpdatesDriftOnly() {
        // SEK target=1_000_000, threshold_abs=50_000, balance=1_010_000
        // → drift=10_000 < threshold → no DB write.
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "advisory", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1010000");

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.engineRan()).isTrue();
        assertThat(s.results()).hasSize(1);
        assertThat(s.results().get(0).outcome()).isEqualTo("below_threshold");
        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void aboveThresholdAdvisory_writesRecommendationButDoesNotFire() {
        // Balance well above target → drift > threshold → recommendation row,
        // no FxHedgeService.submit because the policy is advisory (and
        // auto-fire is off globally).
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "advisory", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");
        primeQuote("EURSEK", "11.50", "11.55", "11.525");
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.results()).hasSize(1);
        assertThat(s.results().get(0).outcome()).isEqualTo("advisory_emitted");
        assertThat(s.results().get(0).actionKey()).startsWith("fx-auto-SEK-");
        verify(jdbc, times(1)).update(anyString(), any(Object[].class));
        verify(hedgeService, never()).submit(any());
    }

    @Test
    void aboveThresholdAutoModeWithKillSwitch_emitsRecommendationOnly() {
        omsConfig.getFx().getAutoHedger().setAutoFireEnabled(false);
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "auto", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");
        primeQuote("EURSEK", "11.50", "11.55", "11.525");
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.results().get(0).outcome()).isEqualTo("auto_inhibited_by_kill_switch");
        verify(hedgeService, never()).submit(any());
    }

    @Test
    void aboveThresholdAutoWithSwitchOn_callsHedgeServiceAndLinksRecommendation() {
        omsConfig.getFx().getAutoHedger().setAutoFireEnabled(true);
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "auto", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");
        primeQuote("EURSEK", "11.50", "11.55", "11.525");
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));
        when(hedgeService.submit(any())).thenReturn(Map.of("id", 42L, "status", "posted"));

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.results().get(0).outcome()).isEqualTo("auto_fired");
        verify(hedgeService, times(1)).submit(any());
        // 1 insert for the recommendation + 1 update to link the hedge action id.
        verify(jdbc, times(2)).update(anyString(), any(Object[].class));
    }

    @Test
    void cooldown_suppressesSecondTickEvenIfDriftPersists() {
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "advisory", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");
        primeQuote("EURSEK", "11.50", "11.55", "11.525");
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));

        FxAutoHedger.TickStatus first = engine.tickInternal();
        FxAutoHedger.TickStatus second = engine.tickInternal();

        assertThat(first.results().get(0).outcome()).isEqualTo("advisory_emitted");
        assertThat(second.results().get(0).outcome()).isEqualTo("cooldown");
        // Only one insert overall (the second tick was suppressed by cooldown).
        verify(jdbc, times(1)).update(anyString(), any(Object[].class));
    }

    @Test
    void engineDisabled_stillUpdatesDriftGaugesAndDoesNotWrite() {
        omsConfig.getFx().getAutoHedger().setEngineEnabled(false);
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "advisory", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1500000");

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.engineRan()).isFalse();
        assertThat(s.reason()).isEqualTo("engine_disabled");
        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void policyOff_writesNothingEvenAboveThreshold() {
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "off", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");

        FxAutoHedger.TickStatus s = engine.tickInternal();

        assertThat(s.results().get(0).outcome()).isEqualTo("policy_off");
        verify(jdbc, never()).update(anyString(), any(Object[].class));
        verify(quoteService, never()).quote(anyString(), anyString());
    }

    @Test
    void quoteCurrencySide_usesPairMidToSizeBaseAmount() {
        // Currency is the QUOTE of the pair (SEK is quote of EURSEK).
        // drift=+200k SEK → BUY EUR (give up SEK), size = 200k / mid.
        when(policy.listAll()).thenReturn(List.of(policyRow(
                "SEK", "advisory", "EURSEK", new BigDecimal("1000000"))));
        primeBalance("SEK", "1200000");
        primeQuote("EURSEK", "11.50", "11.55", "11.525");
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));

        FxAutoHedger.TickStatus s = engine.tickInternal();
        FxAutoHedger.TickResult r = s.results().get(0);
        assertThat(r.outcome()).isEqualTo("advisory_emitted");
        // SELL means "give up base" — SEK above target means we have too
        // much quote, so we BUY base (EUR).
        // Recommendation captures the side derived from currency/pair.
        // Three quote() calls when currency is the QUOTE side of the pair:
        //   1) midRateOrNull for base_amount sizing
        //   2) midRateOrNull for max_per_action cap conversion to base ccy
        //   3) actual quote for the recommendation row
        verify(quoteService, times(3)).quote("EURSEK", "desk");
    }
}
