package com.balh.oms.cluster;

import com.balh.oms.AbstractPostgresIntegrationTest;
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

/**
 * Phase 1 closeout integration test for the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>Runs Spring Boot against the JVM-wide Aeron Cluster singleton from
 * {@link AbstractPostgresIntegrationTest} (Phase 1c moved cluster boot into the base class so
 * <em>every</em> order-accept Spring context goes through it). Verifies the full HTTP →
 * cluster admission → Postgres path:
 *
 * <ul>
 *   <li>Fresh {@code POST /internal/v1/orders} commits via the cluster and (after the projector
 *       daemon applies) writes the same {@code orders} / {@code domain_event_outbox} rows the
 *       legacy Chronicle path produced.</li>
 *   <li>A duplicate post (same {@code accountId} + {@code clientIdempotencyKey}) is detected by
 *       the cluster, returns the original {@code orderId}, and does not create a second
 *       {@code orders} row (option (A): cluster gate is authoritative).</li>
 * </ul>
 *
 * <p>Phase 2 slice 2c moves the {@code orders} INSERT out of the ingress transaction; the
 * orders row materialises via the JVM-wide test projector daemon shortly after the HTTP
 * response. Phase 3 slice 3f drops the {@code control_outbox} write too (the projector emits
 * {@code OrderWorking} envelopes from {@code OrderAdmittedEvent} directly), so only
 * {@code domain_event_outbox} (and the optional ledger inflight outbox for BUYs) stays in the
 * ingress transaction.
 *
 * <p>Complement to {@link OmsClusterIngressClientIT} (cluster client in isolation) and
 * {@link com.balh.oms.ingress.OrderIngressServiceClusterGateTest} (Mockito unit test of the gate
 * logic): this is the only test that wires controller + service + cluster client + actual Aeron
 * Cluster + Postgres end-to-end.
 */
class OrderIngressClusterIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final Duration ORDERS_VISIBLE_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration ORDERS_VISIBLE_POLL = Duration.ofMillis(50);

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;

    @Test
    void postOrder_clusterAdmits_andPostgresInserts() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res =
                exchange(jsonRequest(accountId, "phase1-closeout-fresh"));

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getBody()).isNotNull();
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        // domain-fanout still commits inside the ingress transaction; control_outbox was deleted in slice 3f.
        Long domainOutboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?",
                Long.class,
                orderId);
        assertThat(domainOutboxCount)
                .as("ingress transaction still writes domain_event_outbox after slice 3f")
                .isEqualTo(1L);

        // Phase 2 slice 2c: the orders row arrives via the test projector daemon.
        await()
                .atMost(ORDERS_VISIBLE_TIMEOUT)
                .pollInterval(ORDERS_VISIBLE_POLL)
                .untilAsserted(() -> {
                    Long orderCount = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM orders WHERE id = ? AND account_id = ?",
                            Long.class,
                            orderId,
                            accountId);
                    assertThat(orderCount).isEqualTo(1L);
                });
    }

    @Test
    void postOrder_secondPostWithSameKey_isIdempotentAtClusterAndDb() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> first =
                exchange(jsonRequest(accountId, "phase1-closeout-replay"));
        ResponseEntity<Map<String, Object>> second =
                exchange(jsonRequest(accountId, "phase1-closeout-replay"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isNotNull();
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().get("id"))
                .as("cluster idempotency: replay must return the original orderId so callers see a single identity")
                .isEqualTo(first.getBody().get("id"));

        // Cluster idempotency keeps Postgres at one orders row even after projector apply. Wait
        // for the projector to apply (so we don't pre-empt with a 0-row read) before asserting.
        await()
                .atMost(ORDERS_VISIBLE_TIMEOUT)
                .pollInterval(ORDERS_VISIBLE_POLL)
                .untilAsserted(() -> {
                    Long count = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = ?",
                            Long.class,
                            accountId,
                            "phase1-closeout-replay");
                    assertThat(count)
                            .as("Postgres must remain a single row even when the cluster replays the admission")
                            .isEqualTo(1L);
                });
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
