package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.fix.FixMetrics;
import com.balh.oms.reconciler.OutboxReconciler;
import com.balh.oms.tailer.ControlTailer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Slice 4+: HTTP ingress → {@code control_outbox} → {@link OutboxReconciler} (Chronicle append) →
 * {@link ControlTailer} (synthetic apply from outbox payload) → {@code WORKING} → FIX NOS → synthetic ER →
 * {@code FILLED}. Proves the public ingress path composes with the FIX stack (beyond JDBC-only inserts).
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixIngressRoundTripSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort int port;

    @Autowired private TestRestTemplate http;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private OutboxReconciler controlOutboxReconciler;
    @Autowired private ControlTailer controlTailer;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private MeterRegistry meterRegistry;

    @DynamicPropertySource
    static void fixRoundTripProps(DynamicPropertyRegistry registry) {
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        registry.add("oms.fix.socket-connect-port", () -> String.valueOf(FixRoundTripFixture.PORT));
        registry.add(
                "oms.fix.file-store-path",
                () -> FixRoundTripFixture.INITIATOR_STORE.toAbsolutePath().toString());
        registry.add("oms.fix.sender-comp-id", () -> "INITIATOR");
        registry.add("oms.fix.target-comp-id", () -> "ACCEPTOR");
        registry.add("oms.fix.outbound-poll-interval-ms", () -> "25");
        registry.add("oms.fix.venue-id-for-executions", () -> "FIX_IT");
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "false");
    }

    @BeforeEach
    void reset() {
        FixRoundTripAcceptorApplication.resetItHooks();
        jdbc.update("TRUNCATE TABLE orders CASCADE");
    }

    @Test
    void httpPostThenControlApply_sendsNosAndAcceptorFill_reachesFilled() throws Exception {
        UUID accountId = UUID.randomUUID();
        UUID orderId = postOrder(accountId, "fix-http-rt-1");

        controlOutboxReconciler.runOnce();
        String payload = jdbc.queryForObject(
                "SELECT payload::text FROM control_outbox WHERE order_id = ? ORDER BY id LIMIT 1",
                String.class,
                orderId);
        PendingControlEvent ev = objectMapper.readValue(payload, PendingControlEvent.class);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("FILLED"));

        assertThat(jdbc.queryForObject("SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId))
                .isEqualByComparingTo("10");
        assertThat(meterRegistry.counter(FixMetrics.METRIC_NOS_SENT).count()).isPositive();
        assertThat(meterRegistry
                        .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "trade_APPLIED")
                        .count())
                .isPositive();
    }

    private UUID postOrder(UUID accountId, String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonRequest(accountId, idempotencyKey), headers),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return UUID.fromString((String) res.getBody().get("id"));
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
