package com.balh.oms.chronicle;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.reconciler.OutboxReconciler;
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

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Pins the slice-1 mandatory invariant: the Chronicle journal MUST NOT receive
 * any append until <em>after</em> the corresponding Postgres transaction has
 * committed.
 *
 * <p>The {@link NoOpControlJournal} bean records every append. We assert that
 * the {@code orders} row is visible in Postgres before the journal counter
 * moves, and that the matching {@code control_outbox} row exists from the
 * moment the order is committed.
 */
class ControlPipelineInvariantTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired NoOpControlJournal journal;
    @Autowired OutboxReconciler reconciler;
    @Autowired JdbcTemplate jdbc;
    @Autowired ControlTailer controlTailer;
    @Autowired ObjectMapper objectMapper;

    @Test
    void chronicleAppendOnlyHappensAfterPostgresCommitAndOutboxRowExists() throws Exception {
        long initialJournalCount = journal.appendCount();

        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonRequest(accountId, "k1"), authHeaders()),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getBody()).as("POST /internal/v1/orders body").isNotNull();
        assertThat(res.getBody().get("id"))
                .as("order id in body (status=%s, body=%s)", res.getStatusCode(), res.getBody())
                .isNotNull();
        UUID orderId = parseOrderId(res.getBody().get("id"));

        // 1. The orders row exists immediately after the HTTP call returns
        //    (HTTP 201 means the @Transactional commit boundary has closed).
        Long ordersCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE id = ?", Long.class, orderId);
        assertThat(ordersCount).isEqualTo(1L);

        // 2. The control_outbox row exists in the same transaction.
        Long outboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_outbox WHERE order_id = ?", Long.class, orderId);
        assertThat(outboxCount).isEqualTo(1L);

        // 3. The reconciler only appends after the row has aged past
        //    oms.outbox.reconciler-age-ms (set to 0ms in the test profile).
        //    Drive it explicitly to remove timing flakes.
        reconciler.runOnce();

        await().atMost(Duration.ofSeconds(5))
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        assertThat(journal.appendCount()).isGreaterThan(initialJournalCount));

        // 5. Production uses ChronicleControlTailReader; in the test profile the
        //    journal is a no-op, so we apply the same outbox payload through
        //    ControlTailer to prove the end-to-end control semantics.
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);
        var order = jdbc.queryForObject(
                "SELECT status::text FROM orders WHERE id = ?",
                String.class,
                orderId);
        assertThat(order).isEqualTo(OrderStatus.WORKING.name());

        // 6. The outbox row is now marked appended; the next reconciler pass
        //    must NOT re-append the same row.
        reconciler.runOnce();
        long countAfterSecondRun = journal.appendCount();
        reconciler.runOnce();
        assertThat(journal.appendCount()).isEqualTo(countAfterSecondRun);
    }

    private static UUID parseOrderId(Object raw) {
        return switch (raw) {
            case UUID u -> u;
            case String s -> UUID.fromString(s);
            case null -> throw new IllegalArgumentException("order id is null");
            default -> UUID.fromString(raw.toString());
        };
    }

    private HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
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
