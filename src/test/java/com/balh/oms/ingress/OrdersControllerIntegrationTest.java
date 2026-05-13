package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.persistence.OrdersRepository;
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

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OrdersControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    /**
     * Phase 2 slice 2c: HTTP returns 200/201 on cluster commit; the orders row + outbox / fanout
     * side-tables materialise via the JVM-wide test projector daemon shortly after. 20s budget
     * absorbs the projector connect cost on the first test in a JVM.
     */
    private static final Duration ORDERS_VISIBLE_TIMEOUT = Duration.ofSeconds(20);

    private static final Duration ORDERS_VISIBLE_POLL = Duration.ofMillis(50);

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired OrdersRepository orders;
    @Autowired JdbcTemplate jdbc;

    @Test
    void rejectsRequestsWithoutInternalApiKey() {
        ResponseEntity<String> res = http.postForEntity(
                "http://localhost:" + port + "/internal/v1/orders",
                jsonRequest(UUID.randomUUID(), "key-1"),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void validationFailureReturnsRejectEnvelope() {
        UUID accountId = UUID.randomUUID();
        String longKey = "k".repeat(300);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<ApiErrorResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonRequest(accountId, longKey), headers),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().rejectCode()).isEqualTo("INTERNAL_ERROR");
        assertThat(res.getBody().message()).isEqualTo("validation_failed");
    }

    @Test
    void createsOrderAndWritesOutboxRowInsideSameTransaction() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = exchange(jsonRequest(accountId, "key-1"));

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getBody()).isNotNull();
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        // The domain-fanout row is still written inside the ingress transaction (control_outbox
        // was deleted in slice 3f); it commits before the controller returns.
        Long domainOutboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?", Long.class, orderId);
        assertThat(domainOutboxCount).isEqualTo(1L);

        // Phase 2 slice 2c: the orders row arrives via the test projector daemon (the production
        // ingress no longer writes it). HTTP 201 means cluster-committed; await the projection.
        await()
                .atMost(ORDERS_VISIBLE_TIMEOUT)
                .pollInterval(ORDERS_VISIBLE_POLL)
                .untilAsserted(() -> {
                    var stored = orders.findById(orderId).orElseThrow();
                    assertThat(stored.accountId()).isEqualTo(accountId);
                });
    }

    @Test
    void duplicateIdempotencyKeyReturnsExistingOrder() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> first = exchange(jsonRequest(accountId, "key-1"));
        ResponseEntity<Map<String, Object>> second = exchange(jsonRequest(accountId, "key-1"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));

        // Cluster idempotency keeps Postgres at one orders row even after projector apply. Await
        // the projector apply, then assert the row count is exactly one.
        await()
                .atMost(ORDERS_VISIBLE_TIMEOUT)
                .pollInterval(ORDERS_VISIBLE_POLL)
                .untilAsserted(() -> {
                    Long count = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = 'key-1'",
                            Long.class,
                            accountId);
                    assertThat(count).isEqualTo(1L);
                });

        UUID orderId = UUID.fromString((String) first.getBody().get("id"));
        Long duplicateRejectAudit = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_decisions WHERE order_id = ? AND reject_code::text = 'RISK_DUPLICATE'",
                Long.class,
                orderId);
        assertThat(duplicateRejectAudit).isZero();
    }

    @Test
    void getOrderRejectsWithoutInternalApiKey() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> created = exchange(jsonRequest(accountId, "get-key-1"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) created.getBody().get("id"));

        // GET reads from the orders table; the auth check rejects before the read so we don't need
        // to await the projector here.
        ResponseEntity<String> res = http.getForEntity(
                "http://localhost:" + port + "/internal/v1/orders/" + orderId,
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void getOrderReturnsBodyWhenKeyPresent() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> created = exchange(jsonRequest(accountId, "get-key-2"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) created.getBody().get("id"));

        // Phase 2 slice 2c: orders row arrives via the test projector daemon. Wait for the row to
        // be visible before issuing the GET, otherwise GET returns 404.
        awaitOrderVisible(orderId);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders/" + orderId,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("id")).isEqualTo(orderId.toString());
        assertThat(res.getBody().get("accountId")).isEqualTo(accountId.toString());
        assertThat(res.getBody().get("settlementStatus")).isNull();
    }

    @Test
    void getOrder_returnsAggregatedSettlementStatusFromTradeExecutions() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> created = exchange(jsonRequest(accountId, "get-key-settlement"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        UUID orderId = UUID.fromString((String) created.getBody().get("id"));

        // The executions row used to share an FK to orders(id); V25 dropped it in slice 2c, but we
        // still want the orders row materialised before the GET so the controller can join with
        // executions and produce the aggregated settlementStatus.
        awaitOrderVisible(orderId);

        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('settling' AS execution_settlement_status)
                        )
                        """,
                orderId,
                accountId,
                "vref-settle-" + orderId);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders/" + orderId,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("settlementStatus")).isEqualTo("settling");
    }

    /**
     * Awaits the projector daemon to apply the orders row for {@code orderId}. Phase 2 slice 2c
     * removed the synchronous orders write from {@link OrderIngressService}; row arrival now lags
     * the HTTP response by one projector poll cycle.
     */
    private void awaitOrderVisible(UUID orderId) {
        await()
                .atMost(ORDERS_VISIBLE_TIMEOUT)
                .pollInterval(ORDERS_VISIBLE_POLL)
                .untilAsserted(() -> assertThat(orders.findById(orderId)).isPresent());
    }

    @Test
    void getOrderReturns404WhenMissing() {
        UUID missing = UUID.fromString("00000000-0000-4000-8000-000000000099");
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<String> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders/" + missing,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void rejectsLedgerBalanceWithoutLedgerIntegrationEnabled() {
        UUID accountId = UUID.randomUUID();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        String body = """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "lb-no-ledger",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "1",
                  "limitPrice": "10.00",
                  "timeInForce": "DAY",
                  "ledgerBalanceId": "balance_x",
                  "ledgerIdentityId": "id-1"
                }
                """.formatted(accountId);
        ResponseEntity<ApiErrorResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().message()).isEqualTo("ledger_verification_unavailable");
    }

    private ResponseEntity<Map<String, Object>> exchange(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        return http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                new ParameterizedTypeReference<>() {});
    }

    private String jsonRequest(UUID accountId, String key) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "10",
                  "limitPrice": "150.00",
                  "timeInForce": "DAY"
                }
                """.formatted(accountId, key);
    }
}
