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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Vendor mids and markups are keyed on {@code AUDUSD}; wallet/BFF requests
 * {@code USDAUD} (sell USD → receive AUD).
 */
@ExtendWith(MockitoExtension.class)
class FxQuoteServiceInversePairTest {

    private static final Instant NOW = Instant.parse("2026-05-22T12:00:00Z");

    @Mock
    JdbcTemplate jdbc;

    private FxQuoteService service;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        OmsConfig config = new OmsConfig();
        FxMarkupOverridesService overrides = new FxMarkupOverridesService(
                mock(JdbcTemplate.class), clock, config, new SimpleMeterRegistry(), 60_000L);
        overrides.primeCache(List.of());
        FxTierKillsService tierKills = new FxTierKillsService(
                mock(JdbcTemplate.class), clock, config, new SimpleMeterRegistry(), 60_000L);
        tierKills.primeCache(List.of());
        service = new FxQuoteService(jdbc, clock, config, overrides, tierKills, new SimpleMeterRegistry());
    }

    @Test
    void quote_usdAud_usesAudUsdStubAndMarkups() {
        when(jdbc.queryForList(any(), eq(BigDecimal.class), eq("AUDUSD"), eq("ASK"), eq("default")))
                .thenReturn(List.of(new BigDecimal("30.00")));
        when(jdbc.queryForList(any(), eq(BigDecimal.class), eq("AUDUSD"), eq("BID"), eq("default")))
                .thenReturn(List.of(new BigDecimal("30.00")));

        Map<String, Object> body = service.quote("USDAUD", "default");

        assertThat(body.get("pair")).isEqualTo("USDAUD");
        assertThat(body.get("source")).isEqualTo("internal-bbo-stub-inverse");
        assertThat(body.get("vendorPair")).isEqualTo("AUDUSD");
        BigDecimal bid = new BigDecimal((String) body.get("bid"));
        assertThat(bid).isGreaterThan(new BigDecimal("1.40"));
    }
}
