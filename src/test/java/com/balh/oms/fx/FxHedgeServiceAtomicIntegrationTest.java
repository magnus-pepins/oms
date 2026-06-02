package com.balh.oms.fx;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire-level coverage for {@link FxHedgeService#submit} using the new
 * {@code POST /transactions/bulk?atomic=true} path.
 *
 * <p>Two invariants the previous two-call {@code /transactions} path
 * could not guarantee, both proven here:
 *
 * <ol>
 *   <li><b>Atomicity:</b> a leg-2 (or whole-batch) ledger failure leaves
 *       {@code fx_hedge_actions.status='failed'} with no partial-leg
 *       commit visible to OMS. The ledger's
 *       {@code BulkController.runAtomic} reverses already-applied legs
 *       on the cluster side; OMS only needs to mark the audit row.</li>
 *   <li><b>Single round-trip:</b> exactly one POST to
 *       {@code /transactions/bulk} per hedge, regardless of leg count.</li>
 * </ol>
 */
class FxHedgeServiceAtomicIntegrationTest extends AbstractPostgresIntegrationTest {

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerHedge(DynamicPropertyRegistry registry) {
        synchronized (FxHedgeServiceAtomicIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "atomic-it-key");
        registry.add("oms.fx.module-enabled", () -> "true");
        registry.add("oms.fx.hedge-hooks-enabled", () -> "true");
    }

    @AfterAll
    static void stopLedgerWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @Autowired FxHedgeService hedgeService;
    @Autowired FxQuoteService quoteService;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void reset() {
        ledgerWireMock.resetAll();
        jdbc.update("DELETE FROM fx_hedge_actions");
    }

    @Test
    void happyPath_postsOnceAsAtomicBulkAndRecordsBatchId() {
        // Stub bulk endpoint — success body shape matches BulkController.bulkTransactions.
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions/bulk"))
                .withHeader("Authorization", equalTo("Bearer atomic-it-key"))
                .withRequestBody(matchingJsonPath("$.atomic", equalTo("true")))
                .withRequestBody(matchingJsonPath("$.inflight", equalTo("false")))
                .withRequestBody(matchingJsonPath("$.transactions[0].allowOverdraft", equalTo("true")))
                .withRequestBody(matchingJsonPath("$.transactions[1].allowOverdraft", equalTo("true")))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"batch_id\":\"b-atomic-ok-1\",\"status\":\"applied\",\"transaction_count\":2}")));

        Map<String, Object> result = hedgeService.submit(new FxHedgeService.HedgeRequest(
                "atomic-ok-key-1",
                "it-operator",
                "EURUSD",
                "BUY",
                "desk",
                /* quoteId */ null,
                new BigDecimal("100.00"),
                "balance_base_eur",
                "balance_quote_usd",
                "atomic IT happy path",
                "suspense"));

        assertThat(result.get("status")).isEqualTo("posted");
        assertThat(result.get("ledgerTransactionId")).isEqualTo("b-atomic-ok-1");
        ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions/bulk")));
    }

    @Test
    void atomicLedgerFailure_marksHedgeFailed_withoutPartialLegCommit() {
        // Ledger's BulkController.runAtomic returns 400 ALL_FAILED + errors[]
        // when the first leg rejects (insufficient funds / payment rule / etc).
        // It also handles the leg-2-failed-after-leg-1-applied case internally
        // by issuing reversal commands; OMS sees a single non-applied response
        // either way and must surface the failure on the audit row.
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions/bulk"))
                .withHeader("Authorization", equalTo("Bearer atomic-it-key"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                                "{\"error\":\"ALL_FAILED\",\"batch_id\":\"b-atomic-fail-1\","
                                        + "\"errors\":[\"Atomic batch failed: INSUFFICIENT_FUNDS:"
                                        + " available=-1000 requested=200\"]}")));

        Map<String, Object> result = hedgeService.submit(new FxHedgeService.HedgeRequest(
                "atomic-fail-key-1",
                "it-operator",
                "EURUSD",
                "BUY",
                "desk",
                /* quoteId */ null,
                new BigDecimal("100.00"),
                "balance_base_eur",
                "balance_quote_usd",
                "atomic IT failure path",
                "suspense"));

        assertThat(result.get("status")).isEqualTo("failed");
        assertThat(result.get("ledgerTransactionId")).isNull();
        assertThat(((String) result.get("failureReason"))).contains("INSUFFICIENT_FUNDS");
        // Same single round-trip on the failure branch.
        ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions/bulk")));
    }
}
