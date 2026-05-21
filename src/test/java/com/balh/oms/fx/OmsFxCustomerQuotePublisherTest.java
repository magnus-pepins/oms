package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pure-logic tests for {@link OmsFxCustomerQuotePublisher}.
 *
 * <p>Scope: markup math + waterfall lookup. We intentionally do not boot a
 * broker here — the MQTT publish path is exercised in the pop bench (see
 * §11.5.6 Phase 2 of plans/oms-fix-gateway-and-settlement.md for the smoke
 * test).
 */
@ExtendWith(MockitoExtension.class)
class OmsFxCustomerQuotePublisherTest {

    @Mock OmsFxMidSubscriber midSubscriber;
    @Mock JdbcTemplate jdbc;

    private static final Instant NOW = Instant.parse("2026-05-20T12:00:00Z");
    private Clock clock;
    private FxMarkupOverridesService overrides;
    private FxTierKillsService tierKills;
    private OmsFxCustomerQuotePublisher publisher;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(NOW, ZoneOffset.UTC);
        overrides = new FxMarkupOverridesService(
                mock(JdbcTemplate.class), clock, new OmsConfig(), new SimpleMeterRegistry(), 60_000L);
        // Empty override cache by default so the existing math/waterfall
        // tests below see exactly the rate-card numbers.
        overrides.primeCache(List.of());
        tierKills = new FxTierKillsService(
                mock(JdbcTemplate.class), clock, new OmsConfig(), new SimpleMeterRegistry(), 60_000L);
        // No active kills by default — every (pair, tier) publishes.
        tierKills.primeCache(List.of());
        publisher = new OmsFxCustomerQuotePublisher(
                midSubscriber,
                jdbc,
                clock,
                new ObjectMapper(),
                overrides,
                tierKills,
                new SimpleMeterRegistry(),
                "tcp://localhost:1883",
                "",
                "",
                "test-prefix",
                "basic,elite",
                /*tickPeriodMs*/      1000L,
                /*maxMidAgeMs*/       30_000L,
                /*markupRefreshMs*/   60_000L,
                "fx/{base}/{quote}/customer/{tier}/quote",
                /*retained*/          true,
                /*qos*/               0);
    }

    @Test
    void applyMarkup_buildsBidBelowAndAskAboveMid() {
        // Reference: FxQuoteService.applyMarkup. 20 bps on EURUSD mid 1.0850 →
        //   bid = 1.0850 * (1 - 0.0020) = 1.08283
        //   ask = 1.0850 * (1 + 0.0020) = 1.08717
        BigDecimal mid = new BigDecimal("1.0850");
        BigDecimal bps = new BigDecimal("20.00");
        BigDecimal bid = OmsFxCustomerQuotePublisher.applyMarkup(mid, bps, -1);
        BigDecimal ask = OmsFxCustomerQuotePublisher.applyMarkup(mid, bps, +1);
        assertThat(bid.setScale(5, RoundingMode.HALF_UP).toPlainString()).isEqualTo("1.08283");
        assertThat(ask.setScale(5, RoundingMode.HALF_UP).toPlainString()).isEqualTo("1.08717");
    }

    @Test
    void applyMarkup_zeroBpsReturnsMid() {
        BigDecimal mid = new BigDecimal("1.2345");
        BigDecimal bid = OmsFxCustomerQuotePublisher.applyMarkup(mid, BigDecimal.ZERO, -1);
        BigDecimal ask = OmsFxCustomerQuotePublisher.applyMarkup(mid, BigDecimal.ZERO, +1);
        assertThat(bid.setScale(4, RoundingMode.HALF_UP)).isEqualByComparingTo("1.2345");
        assertThat(ask.setScale(4, RoundingMode.HALF_UP)).isEqualByComparingTo("1.2345");
    }

    @Test
    void lookupMarkup_returnsTierSpecificRowWhenPresent() {
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        publisher.primeMarkupCache(cache);

        assertThat(publisher.lookupMarkup("EURUSD", "BID", "basic")).isEqualByComparingTo("20.00");
        assertThat(publisher.lookupMarkup("EURUSD", "ASK", "elite")).isEqualByComparingTo("10.00");
    }

    @Test
    void lookupMarkup_fallsBackToDefaultRowWhenTierMissing() {
        // Mirrors FxQuoteService.lookupMarkupBps waterfall: V37/V38 leave many pairs
        // with only `default` rows; the customer-quote lookup must not fail on those.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURSEK", "default", "BID"), new BigDecimal("35.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURSEK", "default", "ASK"), new BigDecimal("35.00"));
        publisher.primeMarkupCache(cache);

        assertThat(publisher.lookupMarkup("EURSEK", "BID", "basic")).isEqualByComparingTo("35.00");
        assertThat(publisher.lookupMarkup("EURSEK", "ASK", "elite")).isEqualByComparingTo("35.00");
    }

    @Test
    void lookupMarkup_returnsNullWhenPairUnknown() {
        publisher.primeMarkupCache(Map.of());
        assertThat(publisher.lookupMarkup("XYZABC", "BID", "basic")).isNull();
    }

    @Test
    void markupLoadTiers_alwaysIncludesDefault() {
        // refreshMarkups() uses this set to build the SQL `tier = ANY(?)` array.
        // `default` must be present so the lookupMarkup() waterfall has data
        // for pairs that only carry tier='default' rows (USDMXN, EURAUD, ...).
        assertThat(publisher.markupLoadTiers()).contains("basic", "elite", "default");
    }

    @Test
    void markupLoadTiers_doesNotDuplicateDefaultWhenAlreadyConfigured() {
        OmsFxCustomerQuotePublisher withDefault = new OmsFxCustomerQuotePublisher(
                midSubscriber, jdbc, Clock.fixed(Instant.parse("2026-05-20T12:00:00Z"), ZoneOffset.UTC),
                new ObjectMapper(), overrides, tierKills, new SimpleMeterRegistry(),
                "tcp://localhost:1883", "", "", "test-prefix",
                "basic,default,elite",
                1000L, 30_000L, 60_000L,
                "fx/{base}/{quote}/customer/{tier}/quote", true, 0);
        assertThat(withDefault.markupLoadTiers()).containsExactly("basic", "default", "elite");
    }

    @Test
    void lookupMarkup_addsActiveOverrideOnTopOfBase() {
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        publisher.primeMarkupCache(cache);
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("5.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        // BID gets the override; ASK does not (override is BID-scoped)
        assertThat(publisher.lookupMarkup("EURUSD", "BID", "elite")).isEqualByComparingTo("15.00");
        assertThat(publisher.lookupMarkup("EURUSD", "ASK", "elite")).isEqualByComparingTo("10.00");
    }

    @Test
    void lookupMarkup_overrideAppliesEvenWhenBaseFallsBackToDefault() {
        // Operator widens "elite" on EURSEK. Rate-card only has tier=default.
        // Override intent ("widen elite") is tier-scoped, not row-scoped — it
        // still applies, on top of the resolved default base.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURSEK", "default", "BID"), new BigDecimal("35.00"));
        publisher.primeMarkupCache(cache);
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURSEK", "BID", "elite", new BigDecimal("4.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(publisher.lookupMarkup("EURSEK", "BID", "elite")).isEqualByComparingTo("39.00");
    }

    @Test
    void lookupMarkup_clampsAtZeroWhenNegativeOverrideExceedsBase() {
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("3.00"));
        publisher.primeMarkupCache(cache);
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("-10.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(publisher.lookupMarkup("EURUSD", "BID", "elite")).isEqualByComparingTo("0");
    }

    @Test
    void lookupMarkup_returnsNullWhenBaseMissingEvenWithOverride() {
        // An override cannot conjure a quote on a pair that has no rate-card.
        publisher.primeMarkupCache(Map.of());
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "XYZABC", "BID", "elite", new BigDecimal("100.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(publisher.lookupMarkup("XYZABC", "BID", "elite")).isNull();
    }

    @Test
    void status_surfacesOverridesSnapshot() {
        publisher.primeMarkupCache(Map.of(
                new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00")));
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "basic", new BigDecimal("5.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        OmsFxCustomerQuotePublisher.PublisherStatus s = publisher.status();
        assertThat(s.overrides().cachedRowCount()).isEqualTo(1);
        assertThat(s.overrides().activeCount()).isEqualTo(1);
        assertThat(s.overrides().nextExpiryEpochMs()).isEqualTo(NOW.plusSeconds(60).toEpochMilli());
    }

    @Test
    void previewQuoteGrid_buildsRowsFromMidSnapshotAcrossConfiguredTiers() {
        // EURUSD has a per-tier rate card (basic 20 / elite 10). GBPUSD only has
        // a `default` rate card — both rows should appear because the publisher
        // waterfalls tier=elite to tier=default for GBPUSD. This is the same
        // surface the trading-desk "Live tier quotes" panel renders (plan A1).
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("GBPUSD", "default", "BID"), new BigDecimal("35.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("GBPUSD", "default", "ASK"), new BigDecimal("35.00"));
        publisher.primeMarkupCache(cache);

        long fresh = NOW.toEpochMilli() - 500;
        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), fresh),
                "GBPUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.2500"), fresh)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();

        assertThat(g.rows()).hasSize(2);
        assertThat(g.rows().get(0).pair()).isEqualTo("EURUSD");
        assertThat(g.rows().get(1).pair()).isEqualTo("GBPUSD");
        assertThat(g.mqttConnected()).isFalse(); // no broker in unit test
        assertThat(g.tiers()).containsExactly("basic", "elite");

        OmsFxCustomerQuotePublisher.PairRow eurusd = g.rows().get(0);
        assertThat(eurusd.stale()).isFalse();
        assertThat(eurusd.tiers().get("basic").bidMarkupBps()).isEqualTo("20.00");
        assertThat(eurusd.tiers().get("elite").bidMarkupBps()).isEqualTo("10.00");
        assertThat(eurusd.tiers().get("basic").overrideApplied()).isFalse();
        // Quotes match the standalone applyMarkup math (20 bps × 1.0850).
        assertThat(new BigDecimal(eurusd.tiers().get("basic").bid()))
                .isEqualByComparingTo("1.08283000");
        assertThat(new BigDecimal(eurusd.tiers().get("basic").ask()))
                .isEqualByComparingTo("1.08717000");
    }

    @Test
    void previewQuoteGrid_flagsStaleMidsButStillReturnsThem() {
        // The vendor publisher silent-stall failure mode (P6 observability): we
        // want the operator to see the stale row, not have it disappear from
        // the panel, otherwise a gap is invisible.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        publisher.primeMarkupCache(cache);

        long stale = NOW.toEpochMilli() - 60_000L; // older than maxMidAgeMs=30_000
        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), stale)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        assertThat(g.rows()).hasSize(1);
        assertThat(g.rows().get(0).stale()).isTrue();
        assertThat(g.rows().get(0).midAgeMs()).isGreaterThanOrEqualTo(30_000L);
        // Markup still resolved; the UI decides whether to grey out a stale row.
        assertThat(g.rows().get(0).tiers().get("basic").bid()).isNotNull();
    }

    @Test
    void previewQuoteGrid_marksOverrideAppliedWhenAdditiveNonZero() {
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        publisher.primeMarkupCache(cache);
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("5.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), NOW.toEpochMilli() - 500)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        OmsFxCustomerQuotePublisher.TierQuote elite = g.rows().get(0).tiers().get("elite");
        assertThat(elite.overrideApplied()).isTrue();
        // BID picks up the +5 bps override; ASK is untouched.
        assertThat(elite.bidMarkupBps()).isEqualTo("15.00");
        assertThat(elite.askMarkupBps()).isEqualTo("10.00");
    }

    @Test
    void previewQuoteGrid_emitsNullCellWhenTierHasNoBaseMarkup() {
        // Publisher is configured for tiers={basic,elite} but the cache only
        // carries `basic` — the elite column should render as a null cell
        // (UI shows "—") instead of being silently dropped.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        publisher.primeMarkupCache(cache);

        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), NOW.toEpochMilli() - 500)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        OmsFxCustomerQuotePublisher.TierQuote elite = g.rows().get(0).tiers().get("elite");
        assertThat(elite.bid()).isNull();
        assertThat(elite.ask()).isNull();
    }

    @Test
    void previewQuoteGrid_flagsKilledTierCellsAndStillShowsPriceForOperatorContext() {
        // A kill stops the MQTT publish for that (pair, tier), but the
        // snapshot must still show what the price *would* be — operator
        // needs to see the price they're hiding from customers, so they
        // can sanity-check the kill before approving / revoking.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        publisher.primeMarkupCache(cache);
        // Wildcard-pair kill on elite: covers EURUSD/elite by matching tier
        // only, exercises the matcher's null-pair branch.
        tierKills.primeCache(List.of(new FxTierKillsService.KillRow(
                null, "elite", NOW.minusSeconds(60), NOW.plusSeconds(60))));

        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), NOW.toEpochMilli() - 500)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        OmsFxCustomerQuotePublisher.PairRow row = g.rows().get(0);
        OmsFxCustomerQuotePublisher.TierQuote basic = row.tiers().get("basic");
        OmsFxCustomerQuotePublisher.TierQuote elite = row.tiers().get("elite");

        assertThat(basic.killed()).isFalse();
        assertThat(basic.bid()).isNotNull(); // basic still publishes

        assertThat(elite.killed()).isTrue();
        assertThat(elite.bid()).isNotNull();  // price still computed
        assertThat(elite.bidMarkupBps()).isEqualTo("10.00"); // markup unchanged
    }

    @Test
    void previewQuoteGrid_scopedPairKillOnlyAffectsThatPair() {
        // Pair-scoped kill: EURUSD/elite killed, GBPUSD/elite untouched.
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "BID"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "elite", "ASK"), new BigDecimal("10.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("GBPUSD", "elite", "BID"), new BigDecimal("12.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("GBPUSD", "elite", "ASK"), new BigDecimal("12.00"));
        publisher.primeMarkupCache(cache);
        tierKills.primeCache(List.of(new FxTierKillsService.KillRow(
                "EURUSD", "elite", NOW.minusSeconds(60), NOW.plusSeconds(60))));

        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), NOW.toEpochMilli() - 500),
                "GBPUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.2500"), NOW.toEpochMilli() - 500)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        OmsFxCustomerQuotePublisher.PairRow eurusd = g.rows().stream()
                .filter(r -> r.pair().equals("EURUSD")).findFirst().orElseThrow();
        OmsFxCustomerQuotePublisher.PairRow gbpusd = g.rows().stream()
                .filter(r -> r.pair().equals("GBPUSD")).findFirst().orElseThrow();

        assertThat(eurusd.tiers().get("elite").killed()).isTrue();
        assertThat(gbpusd.tiers().get("elite").killed()).isFalse();
    }

    @Test
    void previewQuoteGrid_expiredKillDoesNotApply() {
        // A kill row whose validUntil is in the past must NOT show as killed
        // (matches the matchesWindow contract used by the publisher).
        Map<OmsFxCustomerQuotePublisher.MarkupKey, BigDecimal> cache = new HashMap<>();
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"));
        cache.put(new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00"));
        publisher.primeMarkupCache(cache);
        tierKills.primeCache(List.of(new FxTierKillsService.KillRow(
                "EURUSD", "basic", NOW.minusSeconds(120), NOW.minusSeconds(60))));

        when(midSubscriber.snapshot()).thenReturn(Map.of(
                "EURUSD", new OmsFxMidSubscriber.MidSample(new BigDecimal("1.0850"), NOW.toEpochMilli() - 500)));

        OmsFxCustomerQuotePublisher.GridSnapshot g = publisher.previewQuoteGrid();
        assertThat(g.rows().get(0).tiers().get("basic").killed()).isFalse();
    }

    @Test
    void status_reflectsConfiguredTiersAndCacheSize() {
        publisher.primeMarkupCache(Map.of(
                new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "BID"), new BigDecimal("20.00"),
                new OmsFxCustomerQuotePublisher.MarkupKey("EURUSD", "basic", "ASK"), new BigDecimal("20.00")
        ));
        OmsFxCustomerQuotePublisher.PublisherStatus s = publisher.status();
        assertThat(s.tiers()).isEqualTo(List.of("basic", "elite"));
        assertThat(s.markupCacheSize()).isEqualTo(2);
        assertThat(s.publishTickPeriodMs()).isEqualTo(1000L);
        assertThat(s.maxMidAgeMs()).isEqualTo(30_000L);
    }
}
