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

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrdersControllerIntegrationTest extends AbstractPostgresIntegrationTest {

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

        var stored = orders.findById(orderId).orElseThrow();
        assertThat(stored.accountId()).isEqualTo(accountId);

        Long outboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_outbox WHERE order_id = ?", Long.class, orderId);
        assertThat(outboxCount).isEqualTo(1L);
    }

    @Test
    void duplicateIdempotencyKeyReturnsExistingOrder() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> first = exchange(jsonRequest(accountId, "key-1"));
        ResponseEntity<Map<String, Object>> second = exchange(jsonRequest(accountId, "key-1"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));

        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = 'key-1'",
                Long.class, accountId);
        assertThat(count).isEqualTo(1L);
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
