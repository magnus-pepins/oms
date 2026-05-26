package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.reconciler.PredictionMarketLedgerOutboxReconciler;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase B exit: {@link PredictionMarketLedgerOutboxReconciler} posts payout legs to Ledger and marks
 * {@code posted_at}; {@code posting_paused} blocks delivery during the dispute window.
 */
class PredictionMarketLedgerOutboxReconcilerIntegrationTest extends AbstractPostgresIntegrationTest {

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerLedger(DynamicPropertyRegistry registry) {
        synchronized (PredictionMarketLedgerOutboxReconcilerIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "it-key");
        registry.add("oms.ledger.settlement-outbox-reconciler-enabled", () -> "true");
    }

    @AfterAll
    static void stopWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired PredictionMarketLedgerOutboxReconciler reconciler;
    @Autowired TestRestTemplate rest;

    @BeforeEach
    void reset() {
        ledgerWireMock.resetAll();
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        jdbc.update("DELETE FROM prediction_market_ledger_outbox");
        jdbc.update("DELETE FROM venue_contract_resolution");
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances"))
                .withHeader("Authorization", equalTo("Bearer it-key"))
                .withQueryParam("indicator", equalTo("inv-a0000001-0000-4000-8000-000000000002-USD"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[{\"balanceId\":\"bal-it-customer-usd\",\"currency\":\"USD\"}]")));
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions"))
                .withHeader("Authorization", equalTo("Bearer it-key"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"txn-predmkt\"}")));
    }

    @Test
    void reconcilerPostsPayoutAndMarksPosted() {
        long resolutionId = insertResolution(false);
        long outboxId = insertOutbox(resolutionId);

        reconciler.runOnce();

        ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions"))
                .withRequestBody(containing("prediction-payout-")));
        assertThat(jdbc.queryForObject(
                        "SELECT posted_at IS NOT NULL FROM prediction_market_ledger_outbox WHERE id = ?",
                        Boolean.class,
                        outboxId))
                .isTrue();
    }

    @Test
    void reconcilerSkipsWhenPostingPaused() {
        long resolutionId = insertResolution(true);
        insertOutbox(resolutionId);

        reconciler.runOnce();

        ledgerWireMock.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));
    }

    @Test
    void postingPausedEndpointBlocksReconciler() {
        long resolutionId = insertResolution(false);
        long outboxId = insertOutbox(resolutionId);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<Map> pauseResp =
                rest.exchange(
                        "/internal/v1/venue/resolutions/" + resolutionId + "/posting-paused",
                        HttpMethod.PATCH,
                        new HttpEntity<>(Map.of("postingPaused", true), headers),
                        Map.class);
        assertThat(pauseResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        reconciler.runOnce();

        ledgerWireMock.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));
        assertThat(jdbc.queryForObject(
                        "SELECT posted_at IS NULL FROM prediction_market_ledger_outbox WHERE id = ?",
                        Boolean.class,
                        outboxId))
                .isTrue();
    }

    private long insertResolution(boolean postingPaused) {
        return jdbc.queryForObject(
                """
                        INSERT INTO venue_contract_resolution (
                            contract_symbol, outcome, resolution_source, resolution_timestamp,
                            evidence_hash, venue_id, dispute_until, posting_paused, orders_resolved_count
                        ) VALUES (
                            'PREDMKT-TEST-1', 'YES', 'it-oracle', NOW(),
                            ?, 'embedded-venue', NOW() - interval '1 minute', ?, 1
                        ) RETURNING id
                        """,
                Long.class,
                "hash-" + UUID.randomUUID(),
                postingPaused);
    }

    private long insertOutbox(long resolutionId) {
        UUID accountId = UUID.fromString("a0000001-0000-4000-8000-000000000002");
        String payload =
                """
                        {"schemaVersion":2,"template":"prediction_market_binary_resolution",\
                        "accountId":"a0000001-0000-4000-8000-000000000002",\
                        "instrumentSymbol":"PREDMKT-TEST-1","payoutAmount":"10.00","currency":"USD",\
                        "outcome":"YES","collateralIndicator":"@Prediction-Market-Collateral-USD"}
                        """;
        return jdbc.queryForObject(
                """
                        INSERT INTO prediction_market_ledger_outbox (resolution_id, account_id, leg_kind, payload_json)
                        VALUES (?, ?, 'prediction-payout', CAST(? AS JSONB))
                        RETURNING id
                        """,
                Long.class,
                resolutionId,
                accountId,
                payload);
    }
}
