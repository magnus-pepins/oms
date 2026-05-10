package com.balh.oms.marketdata;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RestMarketdataPlatformHttpClientParseTest {

    @Test
    void parseInstrumentSymbols_acceptsJsonArrayAndObjectShapes() {
        OmsConfig cfg = new OmsConfig();
        var om = new ObjectMapper();
        var c = new RestMarketdataPlatformHttpClient(null, cfg, om);

        assertThat(c.parseInstrumentSymbols("[\"aapl\",\"MSFT\"]")).containsExactlyInAnyOrder("AAPL", "MSFT");

        assertThat(c.parseInstrumentSymbols("{\"symbols\":[\"GOOG\"]}"))
                .containsExactly("GOOG");

        assertThat(
                        c.parseInstrumentSymbols(
                                "{\"instruments\":[{\"symbol\":\"ibm\"},{\"symbol\":\"NVDA\"}]}"))
                .containsExactlyInAnyOrder("IBM", "NVDA");
    }

    @Test
    void parseNbbo_readsBidAskAndAsOf() {
        OmsConfig cfg = new OmsConfig();
        var om = new ObjectMapper();
        var c = new RestMarketdataPlatformHttpClient(null, cfg, om);
        Instant asOf = Instant.parse("2026-05-08T12:00:00Z");
        var q =
                c.parseNbbo(
                                "{\"bid\":\"10.5\",\"ask\":\"10.75\",\"asOf\":\"%s\"}"
                                        .formatted(asOf))
                        .orElseThrow();
        assertThat(q.bid()).isEqualByComparingTo("10.5");
        assertThat(q.ask()).isEqualByComparingTo("10.75");
        assertThat(q.asOf()).isEqualTo(asOf);
    }

    @Test
    void parseNbbo_rejectsNonPositive() {
        OmsConfig cfg = new OmsConfig();
        var c = new RestMarketdataPlatformHttpClient(null, cfg, new ObjectMapper());
        assertThat(c.parseNbbo("{\"bid\":\"0\",\"ask\":\"1\"}")).isEmpty();
        assertThat(c.parseNbbo("{\"bid\":1,\"ask\":1}")).isPresent();
    }
}
