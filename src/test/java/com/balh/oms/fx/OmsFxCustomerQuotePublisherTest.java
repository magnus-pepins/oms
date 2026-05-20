package com.balh.oms.fx;

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

    private OmsFxCustomerQuotePublisher publisher;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(Instant.parse("2026-05-20T12:00:00Z"), ZoneOffset.UTC);
        publisher = new OmsFxCustomerQuotePublisher(
                midSubscriber,
                jdbc,
                clock,
                new ObjectMapper(),
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
                new ObjectMapper(), new SimpleMeterRegistry(),
                "tcp://localhost:1883", "", "", "test-prefix",
                "basic,default,elite",
                1000L, 30_000L, 60_000L,
                "fx/{base}/{quote}/customer/{tier}/quote", true, 0);
        assertThat(withDefault.markupLoadTiers()).containsExactly("basic", "default", "elite");
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
