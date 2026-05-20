package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Targeted unit tests for the per-balance currency lookup added in
 * §8.4 of plans/oms-multi-currency-invest-accounts.md.
 *
 * <p>Scope is the {@link RestLedgerInflightReservationClient#resolveBalanceCurrency}
 * waterfall — cache hit, Ledger 2xx with currency, Ledger 404, Ledger
 * 5xx, and the no-balance-id fallback. The {@code POST /transactions}
 * path itself is exercised in the existing end-to-end inflight tests.
 */
class RestLedgerInflightReservationClientTest {

    private WireMockServer wireMock;
    private RestLedgerInflightReservationClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        wireMock = new WireMockServer(wireMockConfig().dynamicPort());
        wireMock.start();
        RestClient http = RestClient.builder().baseUrl(wireMock.baseUrl()).build();
        client = new RestLedgerInflightReservationClient(
                http,
                "test-key",
                objectMapper,
                "balance_destination",
                /*defaultCurrency=*/ "USD",
                /*precision=*/ 100);
    }

    @AfterEach
    void tearDown() {
        if (wireMock != null) {
            wireMock.stop();
        }
    }

    @Test
    void resolveBalanceCurrency_returnsLedgerCurrencyOn2xx() {
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_eur"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"EUR\",\"availableBalance\":\"1000000\"}")));
        String ccy = client.resolveBalanceCurrency("bal_eur");
        assertThat(ccy).isEqualTo("EUR");
    }

    @Test
    void resolveBalanceCurrency_cachesResult_secondCallSkipsHttp() {
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_eur"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"EUR\"}")));
        String first = client.resolveBalanceCurrency("bal_eur");
        String second = client.resolveBalanceCurrency("bal_eur");
        assertThat(first).isEqualTo("EUR");
        assertThat(second).isEqualTo("EUR");
        wireMock.verify(1, WireMock.getRequestedFor(urlPathEqualTo("/balances/bal_eur")));
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_404() {
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_missing"))
                .willReturn(aResponse().withStatus(404).withBody("{\"error\":\"not_found\"}")));
        String ccy = client.resolveBalanceCurrency("bal_missing");
        // Fallback so the POST below still runs; Ledger will surface the
        // real error on the POST path.
        assertThat(ccy).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_5xx() {
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_unstable"))
                .willReturn(aResponse().withStatus(503).withBody("")));
        String ccy = client.resolveBalanceCurrency("bal_unstable");
        assertThat(ccy).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_missing_currency_field() {
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_bad_shape"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"100\"}")));
        String ccy = client.resolveBalanceCurrency("bal_bad_shape");
        assertThat(ccy).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_returnsDefault_whenBalanceIdNullOrBlank() {
        assertThat(client.resolveBalanceCurrency(null)).isEqualTo("USD");
        assertThat(client.resolveBalanceCurrency("")).isEqualTo("USD");
        assertThat(client.resolveBalanceCurrency("   ")).isEqualTo("USD");
    }

    @Test
    void primedCache_skipsHttpEntirely() {
        // Belt-and-braces: even if the WireMock server were down, primed
        // entries take precedence.
        client.primeBalanceCurrencyCache("bal_pre", "GBP");
        wireMock.stop();
        String ccy = client.resolveBalanceCurrency("bal_pre");
        assertThat(ccy).isEqualTo("GBP");
    }

    @Test
    void doesNotCache_404OrTransientFailure() {
        // First call: 404 (real problem). Second call: 200 (resolved).
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_recover"))
                .willReturn(aResponse().withStatus(404)));
        assertThat(client.resolveBalanceCurrency("bal_recover")).isEqualTo("USD");

        wireMock.resetAll();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_recover"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"NOK\"}")));
        assertThat(client.resolveBalanceCurrency("bal_recover")).isEqualTo("NOK");
    }
}
