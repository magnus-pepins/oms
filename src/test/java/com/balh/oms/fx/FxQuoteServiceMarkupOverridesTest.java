package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Markup override parity tests for {@link FxQuoteService#lookupMarkupBps}.
 *
 * <p>Why these exist: the customer drift check (P2.1) compares the
 * client-displayed bid/ask (from the streaming publisher) against the
 * mint-time bid/ask (from this service). Any drift in how the two paths
 * combine base markup + override would 409 every order while an override
 * is active. These tests pin the contract.
 */
@ExtendWith(MockitoExtension.class)
class FxQuoteServiceMarkupOverridesTest {

    private static final Instant NOW = Instant.parse("2026-05-20T12:00:00Z");

    @Mock JdbcTemplate jdbc;

    private FxMarkupOverridesService overrides;
    private FxQuoteService service;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        overrides = new FxMarkupOverridesService(
                mock(JdbcTemplate.class), clock, new SimpleMeterRegistry(), 60_000L);
        overrides.primeCache(List.of());
        service = new FxQuoteService(
                jdbc, clock, new OmsConfig(), overrides, new SimpleMeterRegistry());
    }

    @Test
    void lookupMarkupBps_addsActiveOverrideToBase() {
        when(jdbc.queryForList(anyString(), eq(BigDecimal.class),
                any(), any(), eq("elite")))
                .thenReturn(List.of(new BigDecimal("10.00")));
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("5.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(service.lookupMarkupBps("EURUSD", "BID", "elite")).isEqualByComparingTo("15.00");
    }

    @Test
    void lookupMarkupBps_clampsAtZeroWhenNegativeOverrideExceedsBase() {
        when(jdbc.queryForList(anyString(), eq(BigDecimal.class),
                any(), any(), eq("elite")))
                .thenReturn(List.of(new BigDecimal("3.00")));
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "elite", new BigDecimal("-10.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(service.lookupMarkupBps("EURUSD", "BID", "elite")).isEqualByComparingTo("0");
    }

    @Test
    void lookupMarkupBps_returnsBaseWhenNoOverrideActive() {
        when(jdbc.queryForList(anyString(), eq(BigDecimal.class),
                any(), any(), eq("elite")))
                .thenReturn(List.of(new BigDecimal("10.00")));
        // override exists but for a different tier
        overrides.primeCache(List.of(new FxMarkupOverridesService.OverrideRow(
                "EURUSD", "BID", "basic", new BigDecimal("5.00"),
                NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(service.lookupMarkupBps("EURUSD", "BID", "elite")).isEqualByComparingTo("10.00");
    }

    @Test
    void lookupMarkupBps_wildcardOverrideStacksWithSpecific() {
        when(jdbc.queryForList(anyString(), eq(BigDecimal.class),
                any(), any(), eq("elite")))
                .thenReturn(List.of(new BigDecimal("10.00")));
        // wildcard widening on top of pair-specific widening
        overrides.primeCache(List.of(
                new FxMarkupOverridesService.OverrideRow(
                        "EURUSD", "BID", "elite", new BigDecimal("5.00"),
                        NOW.minusSeconds(60), NOW.plusSeconds(60)),
                new FxMarkupOverridesService.OverrideRow(
                        null, null, null, new BigDecimal("2.00"),
                        NOW.minusSeconds(60), NOW.plusSeconds(60))));

        assertThat(service.lookupMarkupBps("EURUSD", "BID", "elite")).isEqualByComparingTo("17.00");
    }
}
