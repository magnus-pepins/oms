package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.fx.FxMultiLegAtomicityStubService;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FX M3 read paths: quote stub, nostro snapshot (Ledger WireMock), hedge hooks, health tracks, multi-leg rollback.
 */
class FxM3ReadPathIntegrationTest extends AbstractPostgresIntegrationTest {

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerFxM3(DynamicPropertyRegistry registry) {
        synchronized (FxM3ReadPathIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "fx-m3-ledger-key");
        registry.add("oms.fx.module-enabled", () -> "true");
        registry.add("oms.fx.quote-stub-enabled", () -> "true");
        registry.add("oms.fx.nostro-read-enabled", () -> "true");
        registry.add("oms.fx.nostro-balance-ids-csv", () -> "bal_nostro_it");
        registry.add("oms.fx.multi-leg-atomicity-stub-enabled", () -> "true");
        registry.add("oms.fx.hedge-hooks-enabled", () -> "true");
        registry.add("oms.fx.suspense.currencies-csv", () -> "USD,EUR");
        registry.add("oms.fx.suspense.max-abs-csv", () -> "USD=500,EUR=600");
    }

    @AfterAll
    static void stopLedgerWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    FxMultiLegAtomicityStubService fxMultiLegAtomicityStubService;

    @BeforeEach
    void resetWireMock() {
        ledgerWireMock.resetAll();
    }

    @Test
    void healthIncludesTrackMap() {
        ResponseEntity<Map> res = http.exchange(
                fxUrl("/internal/v1/fx/health"),
                HttpMethod.GET,
                internalKeyEntity(),
                Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("moduleEnabled")).isEqualTo(Boolean.TRUE);
        @SuppressWarnings("unchecked")
        Map<String, String> tracks = (Map<String, String>) res.getBody().get("tracks");
        assertThat(tracks.get("quoteIngress")).isEqualTo("stub");
        assertThat(tracks.get("nostroRead")).isEqualTo("live");
        assertThat(tracks.get("multiLegAtomicity")).isEqualTo("stub");
        assertThat(tracks.get("hedgeHooks")).isEqualTo("stub");
    }

    @Test
    void quotesStubReturnsSchemaVersion() {
        ResponseEntity<Map> res = http.exchange(
                fxUrl("/internal/v1/fx/quotes"),
                HttpMethod.GET,
                internalKeyEntity(),
                Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("schemaVersion")).isEqualTo(1);
        assertThat(res.getBody().get("source")).isEqualTo("stub");
    }

    @Test
    void nostroSnapshotReadsLedgerBalances() {
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/bal_nostro_it"))
                .withQueryParam("with_queued", equalTo("true"))
                .withHeader("Authorization", equalTo("Bearer fx-m3-ledger-key"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                                "{\"balanceId\":\"bal_nostro_it\",\"currency\":\"USD\",\"availableBalance\":\"100.00\",\"balance\":\"100.00\",\"identityId\":\"id-it\"}")));

        ResponseEntity<Map> res = http.exchange(
                fxUrl("/internal/v1/fx/nostro/snapshot"),
                HttpMethod.GET,
                internalKeyEntity(),
                Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> balances = (List<Map<String, Object>>) res.getBody().get("balances");
        assertThat(balances).hasSize(1);
        assertThat(balances.get(0).get("currency")).isEqualTo("USD");
        assertThat(balances.get(0).get("availableBalance")).isEqualTo("100.00");
    }

    @Test
    void hedgeHooksStatusOk() {
        ResponseEntity<Map> res = http.exchange(
                fxUrl("/internal/v1/fx/hedge/hooks-status"),
                HttpMethod.GET,
                internalKeyEntity(),
                Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("paused")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void suspenseSnapshotMarksOverLimitAndPassesThroughLedgerErrors() {
        // USD over the configured 500 limit, EUR is a ledger lookup failure (404).
        // RestClient encodes '@' as %40 in path segments.
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/indicator/%40FX-Suspense-USD/currency/USD"))
                .withQueryParam("with_queued", equalTo("true"))
                .withHeader("Authorization", equalTo("Bearer fx-m3-ledger-key"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"-800.00\"}")));
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/indicator/%40FX-Suspense-EUR/currency/EUR"))
                .withQueryParam("with_queued", equalTo("true"))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"error\":\"BALANCE_NOT_FOUND\"}")));

        ResponseEntity<Map> res = http.exchange(
                fxUrl("/internal/v1/fx/suspense/snapshot"),
                HttpMethod.GET,
                internalKeyEntity(),
                Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> balances = (List<Map<String, Object>>) res.getBody().get("balances");
        assertThat(balances).hasSize(2);

        Map<String, Object> usd = balances.stream()
                .filter(b -> "USD".equals(b.get("currency"))).findFirst().orElseThrow();
        assertThat(usd.get("availableBalance")).isEqualTo("-800.00");
        assertThat(usd.get("absAvailable")).isEqualTo("800.00");
        assertThat(usd.get("maxAbsLimit")).isEqualTo("500");
        assertThat(usd.get("overLimit")).isEqualTo(Boolean.TRUE);

        Map<String, Object> eur = balances.stream()
                .filter(b -> "EUR".equals(b.get("currency"))).findFirst().orElseThrow();
        assertThat(eur.get("error")).isNotNull();
        assertThat(eur.get("overLimit")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void multiLegTransactionalRollbackDropsRows() {
        jdbcTemplate.update("TRUNCATE fx_stub_leg_group CASCADE");
        assertThatThrownBy(() -> fxMultiLegAtomicityStubService.createTwoLegStubThenFail(List.of("EUR", "USD")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fx_stub_rollback_probe");
        assertThat(fxMultiLegAtomicityStubService.countLegGroups()).isZero();
    }

    @Test
    void multiLegCommitPersists() {
        jdbcTemplate.update("TRUNCATE fx_stub_leg_group CASCADE");
        fxMultiLegAtomicityStubService.createTwoLegStub(List.of("EUR", "USD"));
        assertThat(fxMultiLegAtomicityStubService.countLegGroups()).isEqualTo(1);
    }

    private String fxUrl(String path) {
        return "http://localhost:" + port + path;
    }

    private static HttpEntity<Void> internalKeyEntity() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-OMS-Internal-Key", "test-key");
        return new HttpEntity<>(h);
    }
}
