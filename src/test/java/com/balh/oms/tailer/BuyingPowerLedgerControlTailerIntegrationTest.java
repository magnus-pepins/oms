package com.balh.oms.tailer;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.reconciler.OutboxReconciler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
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

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Postgres + WireMock Ledger HTTP: buying-power gate on the real
 * {@link ControlTailer} + {@link com.balh.oms.ledger.RestLedgerBalanceClient} stack.
 */
class BuyingPowerLedgerControlTailerIntegrationTest extends AbstractPostgresIntegrationTest {

    /** WireMock stub status simulating Ledger upstream failure (retryable 5xx class). */
    private static final int LEDGER_STUB_HTTP_UNAVAILABLE = 503;

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerLedgerWireMock(DynamicPropertyRegistry registry) {
        synchronized (BuyingPowerLedgerControlTailerIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "ledger-it-key");
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
    @Autowired OutboxReconciler reconciler;
    @Autowired JdbcTemplate jdbc;
    @Autowired ControlTailer controlTailer;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void resetLedgerStubs() {
        ledgerWireMock.resetAll();
    }

    @Test
    void buyWithSufficientLedgerBalanceAdvancesToWorking() throws Exception {
        stubBalance("10000.00");
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = postOrder(accountId, "bp-ok", "balance_it", "1", "100.00");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        reconciler.runOnce();
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        String status = jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(status).isEqualTo("WORKING");
    }

    @Test
    void buyWithInsufficientLedgerBalanceRejected() throws Exception {
        stubBalance("50.00");
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = postOrder(accountId, "bp-reject", "balance_it", "2", "100.00");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        reconciler.runOnce();
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.BUYING_POWER_REJECTED);

        String status = jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(status).isEqualTo("REJECTED");
        String reason = jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(reason).isEqualTo("RISK_BUYING_POWER");
    }

    @Test
    void buyWhenLedgerReturns503RejectedWithInternalError() throws Exception {
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/balance_it"))
                .withQueryParam("with_queued", equalTo("true"))
                .willReturn(aResponse()
                        .withStatus(LEDGER_STUB_HTTP_UNAVAILABLE)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"error\":\"ledger_unavailable\"}")));
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = postOrder(accountId, "bp-503", "balance_it", "1", "10.00");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        reconciler.runOnce();
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.LEDGER_SERVICE_REJECTED);

        String status = jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(status).isEqualTo("REJECTED");
        String reason = jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(reason).isEqualTo("INTERNAL_ERROR");
    }

    private void stubBalance(String availableBalanceJson) {
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/balance_it"))
                .withQueryParam("with_queued", equalTo("true"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"" + availableBalanceJson + "\"}")));
    }

    private ResponseEntity<Map<String, Object>> postOrder(
            UUID accountId,
            String key,
            String ledgerBalanceId,
            String quantity,
            String limitPrice) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        String body = """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "%s",
                  "limitPrice": "%s",
                  "timeInForce": "DAY",
                  "ledgerBalanceId": "%s"
                }
                """.formatted(accountId, key, quantity, limitPrice, ledgerBalanceId);
        return http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                new ParameterizedTypeReference<>() {});
    }
}
