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

    /** Cross-currency client: non-empty dest map forces per-balance currency lookup. */
    private RestLedgerInflightReservationClient crossCurrencyClient() {
        return new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_destination",
                java.util.Map.of("EUR", "balance_eur_dest"),
                "USD",
                100);
    }

    @Test
    void resolveBalanceCurrency_singleCurrencyStack_skipsLookupUsesConfiguredDefault() {
        wireMock.stop();
        assertThat(client.resolveBalanceCurrency("bal_eur")).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_returnsLedgerCurrencyOn2xx() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_eur"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"EUR\",\"availableBalance\":\"1000000\"}")));
        String ccy = cross.resolveBalanceCurrency("bal_eur");
        assertThat(ccy).isEqualTo("EUR");
    }

    @Test
    void resolveBalanceCurrency_cachesResult_secondCallSkipsHttp() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_eur"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"EUR\"}")));
        String first = cross.resolveBalanceCurrency("bal_eur");
        String second = cross.resolveBalanceCurrency("bal_eur");
        assertThat(first).isEqualTo("EUR");
        assertThat(second).isEqualTo("EUR");
        wireMock.verify(1, WireMock.getRequestedFor(urlPathEqualTo("/balances/bal_eur")));
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_404() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_missing"))
                .willReturn(aResponse().withStatus(404).withBody("{\"error\":\"not_found\"}")));
        String ccy = cross.resolveBalanceCurrency("bal_missing");
        // Fallback so the POST below still runs; Ledger will surface the
        // real error on the POST path.
        assertThat(ccy).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_5xx() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_unstable"))
                .willReturn(aResponse().withStatus(503).withBody("")));
        String ccy = cross.resolveBalanceCurrency("bal_unstable");
        assertThat(ccy).isEqualTo("USD");
    }

    @Test
    void resolveBalanceCurrency_falls_back_on_missing_currency_field() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_bad_shape"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"100\"}")));
        String ccy = cross.resolveBalanceCurrency("bal_bad_shape");
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
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        cross.primeBalanceCurrencyCache("bal_pre", "GBP");
        wireMock.stop();
        String ccy = cross.resolveBalanceCurrency("bal_pre");
        assertThat(ccy).isEqualTo("GBP");
    }

    // -----------------------------------------------------------------
    // §8.4 per-currency destination override (cross-currency BUY hold).
    // -----------------------------------------------------------------

    @Test
    void resolveDestinationBalanceId_returnsDefault_whenOverrideMapEmpty() {
        // Default constructor used by setUp(): empty override map → always
        // returns the legacy default. Single-currency stacks must keep
        // working unchanged.
        assertThat(client.resolveDestinationBalanceId("USD")).isEqualTo("balance_destination");
        assertThat(client.resolveDestinationBalanceId("EUR")).isEqualTo("balance_destination");
        assertThat(client.resolveDestinationBalanceId(null)).isEqualTo("balance_destination");
    }

    @Test
    void resolveDestinationBalanceId_picksPerCurrencyEntry() {
        RestLedgerInflightReservationClient c = new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_default_usd",
                java.util.Map.of("EUR", "balance_eur_nostro", "GBP", "balance_gbp_nostro"),
                "USD",
                100);
        assertThat(c.resolveDestinationBalanceId("USD")).isEqualTo("balance_default_usd");
        assertThat(c.resolveDestinationBalanceId("EUR")).isEqualTo("balance_eur_nostro");
        assertThat(c.resolveDestinationBalanceId("GBP")).isEqualTo("balance_gbp_nostro");
    }

    @Test
    void resolveDestinationBalanceId_isCaseInsensitive() {
        // Spring relaxed binding can deliver keys lower- or upper-case.
        RestLedgerInflightReservationClient c = new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_default",
                java.util.Map.of("eur", "balance_eur"),  // lowercase key
                "USD",
                100);
        assertThat(c.resolveDestinationBalanceId("EUR")).isEqualTo("balance_eur");
        assertThat(c.resolveDestinationBalanceId("eur")).isEqualTo("balance_eur");
    }

    @Test
    void resolveDestinationBalanceId_fallsBackToDefault_andWarnsOnce() {
        // Override map exists but doesn't cover this currency — fallback
        // path with a one-shot warn (verified via the package-private
        // putIfAbsent: a second call returns the same dest without
        // re-warning, but we can't easily inspect log here, so just
        // assert behaviour stays consistent).
        RestLedgerInflightReservationClient c = new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_default",
                java.util.Map.of("EUR", "balance_eur"),
                "USD",
                100);
        assertThat(c.resolveDestinationBalanceId("BRL")).isEqualTo("balance_default");
        assertThat(c.resolveDestinationBalanceId("BRL")).isEqualTo("balance_default");
    }

    @Test
    void resolveDestinationBalanceId_ignoresEmptyEntries() {
        // Operators wiring via env may leave optional currencies set to
        // an empty string; the normalisation step must drop those rather
        // than route holds to ""-destination (Ledger 400 with confusing
        // error). Same for null keys.
        java.util.Map<String, String> raw = new java.util.LinkedHashMap<>();
        raw.put("USD", "balance_usd_real");
        raw.put("EUR", "");          // empty value — drop
        raw.put("", "balance_orphan"); // empty key — drop
        raw.put("GBP", "   ");        // whitespace value — drop
        RestLedgerInflightReservationClient c = new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_default",
                raw,
                "USD",
                100);
        assertThat(c.resolveDestinationBalanceId("USD")).isEqualTo("balance_usd_real");
        assertThat(c.resolveDestinationBalanceId("EUR")).isEqualTo("balance_default");
        assertThat(c.resolveDestinationBalanceId("GBP")).isEqualTo("balance_default");
    }

    @Test
    void legacyConstructor_isEquivalentToEmptyOverrideMap() {
        // Belt-and-braces: the existing 6-arg constructor used by tests +
        // any out-of-tree code keeps the old single-currency semantics.
        RestLedgerInflightReservationClient c = new RestLedgerInflightReservationClient(
                RestClient.builder().baseUrl(wireMock.baseUrl()).build(),
                "test-key",
                objectMapper,
                "balance_legacy",
                "USD",
                100);
        assertThat(c.resolveDestinationBalanceId("USD")).isEqualTo("balance_legacy");
        assertThat(c.resolveDestinationBalanceId("EUR")).isEqualTo("balance_legacy");
        assertThat(c.defaultDestinationBalanceId()).isEqualTo("balance_legacy");
    }

    @Test
    void doesNotCache_404OrTransientFailure() {
        RestLedgerInflightReservationClient cross = crossCurrencyClient();
        // First call: 404 (real problem). Second call: 200 (resolved).
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_recover"))
                .willReturn(aResponse().withStatus(404)));
        assertThat(cross.resolveBalanceCurrency("bal_recover")).isEqualTo("USD");

        wireMock.resetAll();
        wireMock.stubFor(get(urlPathEqualTo("/balances/bal_recover"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"currency\":\"NOK\"}")));
        assertThat(cross.resolveBalanceCurrency("bal_recover")).isEqualTo("NOK");
    }
}
