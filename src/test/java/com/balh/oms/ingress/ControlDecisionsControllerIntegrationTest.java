package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.time.Instant;
import java.util.UUID;
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
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class ControlDecisionsControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void rejectsWithoutInternalApiKey() {
        ResponseEntity<String> res = http.getForEntity(url("?orderId=" + UUID.randomUUID()), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void badRequestWhenNoOrderIdAndNoTimeWindow() {
        ResponseEntity<String> res =
                http.exchange(url(""), HttpMethod.GET, new HttpEntity<>(headers()), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void badRequestWhenTimeOnlyRangeTooLong() {
        Instant from = Instant.parse("2026-01-01T00:00:00Z");
        Instant to = Instant.parse("2026-03-15T00:00:00Z");
        ResponseEntity<String> res =
                http.exchange(
                        url("?from=" + from + "&to=" + to),
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void listsByOrderId() {
        UUID orderId = insertMinimalOrder("cd-1");
        jdbc.update(
                """
                        INSERT INTO control_decisions (order_id, order_version_before, outcome, reject_code, stage, detail)
                        VALUES (?, 0, 'REJECT', 'RISK_KILL_SWITCH'::reject_code, 'CONTROL', '{"k":1}'::jsonb)
                        """,
                orderId);

        ResponseEntity<ControlDecisionsPageResponse> res =
                http.exchange(
                        url("?orderId=" + orderId + "&limit=10"),
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        assertThat(res.getBody().items().getFirst().orderId()).isEqualTo(orderId);
        assertThat(res.getBody().items().getFirst().outcome()).isEqualTo("REJECT");
        assertThat(res.getBody().items().getFirst().rejectCode()).isEqualTo("RISK_KILL_SWITCH");
        assertThat(res.getBody().items().getFirst().detail()).contains("k");
    }

    @Test
    void listsByTimeWindowWithoutOrderId() {
        UUID orderId = insertMinimalOrder("cd-2");
        Instant decided = Instant.parse("2026-06-01T12:00:00Z");
        jdbc.update(
                """
                        INSERT INTO control_decisions (order_id, order_version_before, outcome, reject_code, stage, detail, decided_at)
                        VALUES (?, 0, 'PASS', NULL, 'CONTROL', '{}'::jsonb, ?)
                        """,
                orderId,
                java.sql.Timestamp.from(decided));

        Instant from = Instant.parse("2026-06-01T00:00:00Z");
        Instant to = Instant.parse("2026-06-02T00:00:00Z");
        ResponseEntity<ControlDecisionsPageResponse> res =
                http.exchange(
                        url("?from=" + from + "&to=" + to),
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        assertThat(res.getBody().items().getFirst().outcome()).isEqualTo("PASS");
    }

    private UUID insertMinimalOrder(String idemKey) {
        UUID id = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id
                        ) VALUES (
                          ?, ?, ?, 0, 0, 'NEW', 'BUY', 'AAPL', CAST(1 AS NUMERIC), CAST(10 AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'hash', NULL
                        )
                        """,
                id,
                accountId,
                idemKey);
        return id;
    }

    private String url(String query) {
        String q = query.startsWith("?") ? query : (query.isEmpty() ? "" : "?" + query);
        return "http://localhost:" + port + "/internal/v1/control-decisions" + q;
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }
}
