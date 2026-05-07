package com.balh.oms.reconciler;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.tailer.ControlTailer;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@code domain_event_outbox} + {@link DomainFanoutReconciler}: envelopes are
 * written with commits and {@code published_at} is set only after {@link com.balh.oms.events.FanoutClient}
 * delivery (no-op transport in the test profile).
 */
class DomainFanoutOutboxIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired DomainFanoutReconciler domainFanoutReconciler;
    @Autowired OutboxReconciler controlOutboxReconciler;
    @Autowired ControlTailer controlTailer;
    @Autowired ObjectMapper objectMapper;

    @Test
    void orderAcceptedEnvelopeIsMarkedPublishedAfterFanoutReconciler() {
        UUID accountId = UUID.randomUUID();
        UUID orderId = postOrder(accountId, "fanout-accept-1");

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?", Long.class, orderId))
                .isEqualTo(1L);

        domainFanoutReconciler.runOnce();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ? AND published_at IS NOT NULL",
                Long.class,
                orderId))
                .isEqualTo(1L);
        String type = jdbc.queryForObject(
                "SELECT envelope_json->>'type' FROM domain_event_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        assertThat(type).isEqualTo("OrderAccepted");
    }

    @Test
    void orderWorkingEnvelopeIsPublishedAfterControlTailer() throws Exception {
        UUID accountId = UUID.randomUUID();
        UUID orderId = postOrder(accountId, "fanout-working-1");

        controlOutboxReconciler.runOnce();
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);
        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo(OrderStatus.WORKING.name());

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?", Long.class, orderId))
                .isEqualTo(2L);

        domainFanoutReconciler.runOnce();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ? AND published_at IS NOT NULL",
                Long.class,
                orderId))
                .isEqualTo(2L);

        List<String> types = jdbc.query(
                "SELECT envelope_json->>'type' AS t FROM domain_event_outbox WHERE order_id = ? ORDER BY id",
                (rs, i) -> rs.getString("t"),
                orderId);
        assertThat(types).containsExactly("OrderAccepted", "OrderWorking");
    }

    private UUID postOrder(UUID accountId, String idempotencyKey) {
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonRequest(accountId, idempotencyKey), authHeaders()),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return UUID.fromString((String) res.getBody().get("id"));
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }

    private static String jsonRequest(UUID accountId, String key) {
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
