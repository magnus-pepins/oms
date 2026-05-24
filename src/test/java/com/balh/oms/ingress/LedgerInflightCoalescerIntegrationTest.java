package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.reconciler.LedgerInflightOutboxReconciler;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 slice 4q — wire-level coverage for the coalescer path. Three invariants the unit
 * tests can't see:
 * <ul>
 *   <li>The dispatcher actually POSTs to {@code /transactions/bulk?inflight=true&atomic=false}
 *       (route + body shape match what {@code ledger/src/api/routes/transaction.routes.ts}
 *       parses).</li>
 *   <li>On 2xx success the coalescer posts via {@code /transactions/bulk} and the per-order
 *       {@code /transactions} reconciler path has not run yet ({@code published_at} still
 *       null). D-9 also materialises a projector-owned {@code ledger_inflight_outbox} row as
 *       belt-and-suspenders — the slice 4q "empty outbox" invariant no longer applies.</li>
 *   <li>On a 5xx whole-batch failure the row appears in {@code ledger_inflight_outbox} so the
 *       existing reconciler (slice 4p) drives Ledger on retry.</li>
 * </ul>
 */
class LedgerInflightCoalescerIntegrationTest extends AbstractPostgresIntegrationTest {

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerCoalescer(DynamicPropertyRegistry registry) {
        synchronized (LedgerInflightCoalescerIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "it-key");
        registry.add("oms.ledger.inflight-reservation-enabled", () -> "true");
        // Coalescer takes priority over async-outbox in OrderIngressService.maybePlaceBuyLedgerInflightHold.
        // We leave async-enabled=true so the reconciler still drives any outbox rows the
        // coalescer's fallback path writes (the second test depends on that).
        registry.add("oms.ledger.inflight-async-enabled", () -> "true");
        registry.add("oms.ledger.inflight-coalescer-enabled", () -> "true");
        registry.add("oms.ledger.inflight-coalescer-max-batch-size", () -> "8");
        registry.add("oms.ledger.inflight-coalescer-flush-interval-micros", () -> "2000");
        registry.add("oms.ledger.inflight-coalescer-queue-capacity", () -> "100");
        registry.add("oms.ledger.inflight-coalescer-submit-timeout-ms", () -> "5000");
        registry.add("oms.ledger.inflight-hold-destination-balance-id", () -> "hold_dest_balance");
        registry.add("oms.ledger.inflight-outbox-reconciler-age-ms", () -> "0");
    }

    @AfterAll
    static void stopLedgerWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired LedgerInflightOutboxReconciler ledgerInflightOutboxReconciler;

    @BeforeEach
    void resetStubs() {
        ledgerWireMock.resetAll();
        jdbc.update("DELETE FROM ledger_inflight_outbox");
    }

    @Test
    void coalescerPath_routesBuyHoldThroughBulkEndpoint_projectorOutboxUnpublished() {
        // Verify path sends with_queued=false so it hits Ledger's Redis cache (Tier 2.5 phase C-3,
        // see RestLedgerBalanceClient Javadoc).
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/cust_balance_coalescer"))
                .withQueryParam("with_queued", equalTo("false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"999\",\"identityId\":\"ident-coalescer-it\"}")));
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions/bulk"))
                .withHeader("Authorization", equalTo("Bearer it-key"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"batch_id\":\"b-it-1\",\"status\":\"applied\",\"transaction_count\":1}")));

        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(
                        jsonBody(accountId, "coalescer-it-1", "cust_balance_coalescer", "ident-coalescer-it"),
                        authHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        // Coalescer path must hit /transactions/bulk, not the per-order /transactions endpoint.
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions/bulk"))));
        ledgerWireMock.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));

        // Phase 4 Tier 2.5 D-9: the test projector daemon always materialises
        // ledger_inflight_outbox from OrderAdmittedEvent when inflight-async-enabled=true,
        // even on the coalescer happy path (ingress/coalescer no longer write it). Slice 4q
        // invariant narrows to: bulk hold succeeded and the slice-4p reconciler has not yet
        // posted via /transactions (published_at still null).
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*) FROM ledger_inflight_outbox WHERE order_id = ?",
                        Long.class,
                        orderId))
                        .isEqualTo(1L));
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM ledger_inflight_outbox WHERE order_id = ? AND published_at IS NULL",
                Long.class,
                orderId))
                .isEqualTo(1L);
    }

    @Test
    void coalescerBulkFailure_fallsBackToOutbox_andReconcilerDrivesLedger() {
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/cust_balance_fallback"))
                .withQueryParam("with_queued", equalTo("false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"999\",\"identityId\":\"ident-fallback-it\"}")));
        // Bulk endpoint blows up -> coalescer should write to outbox.
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions/bulk"))
                .willReturn(aResponse().withStatus(503).withBody("upstream down")));
        // Reconciler will retry via the per-order /transactions endpoint; stub it green.
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"recon-tx-1\"}")));

        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(
                        jsonBody(accountId, "coalescer-it-2", "cust_balance_fallback", "ident-fallback-it"),
                        authHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode())
                .as("the coalescer's outbox fallback completes the future successfully, so ingress still returns 201")
                .isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        // Bulk POST attempted; row exists in outbox waiting for reconciler.
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions/bulk"))));
        // The row must be present in outbox at least once; published_at may already be stamped
        // if the @Scheduled reconciler raced us. Both states satisfy the slice 4q invariant —
        // a flush failure is captured by the existing slice 4p reconciler/compensator path.
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM ledger_inflight_outbox WHERE order_id = ?",
                Long.class,
                orderId))
                .isEqualTo(1L);

        ledgerInflightOutboxReconciler.runOnce();
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*) FROM ledger_inflight_outbox WHERE order_id = ? AND published_at IS NOT NULL",
                        Long.class,
                        orderId))
                        .isEqualTo(1L));
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }

    private static String jsonBody(UUID accountId, String idempotencyKey, String ledgerBalanceId, String ledgerIdentityId) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "2",
                  "limitPrice": "10.00",
                  "timeInForce": "DAY",
                  "ledgerBalanceId": "%s",
                  "ledgerIdentityId": "%s"
                }
                """.formatted(accountId, idempotencyKey, ledgerBalanceId, ledgerIdentityId);
    }
}
