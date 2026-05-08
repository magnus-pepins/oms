package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.fix.FixMetrics;
import com.balh.oms.routing.RouteDispatcher;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Outbound job age: stale WORKING orders are rejected at dequeue without sending NOS (slice 4).
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixOutboundStaleSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private RouteDispatcher routeDispatcher;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @DynamicPropertySource
    static void fixStaleProps(DynamicPropertyRegistry registry) {
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
        registry.add("oms.fix.max-outbound-job-age-ms", () -> "5000");
        registry.add("oms.fix.venue-id-for-executions", () -> "FIX_IT");
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "false");
    }

    @BeforeEach
    void reset() {
        FixRoundTripAcceptorApplication.resetItHooks();
        jdbc.update("TRUNCATE TABLE orders CASCADE");
    }

    @Test
    void staleWorkingOrder_rejectedAtDequeue_noNosSent() {
        UUID orderId = insertWorkingOrder("fix-stale-1", "AAPL", "10", "5.00", 1);
        jdbc.update("UPDATE orders SET accepted_at = NOW() - INTERVAL '2 hours' WHERE id = ?", orderId);

        routeDispatcher.enqueueWorkingOrder(orderId);

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("REJECTED"));

        assertThat(jdbc.queryForObject(
                        "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("FIX_OUTBOUND_JOB_EXPIRED");

        assertThat(FixRoundTripAcceptorApplication.NOS_RECEIVED.get()).isZero();
        assertThat(meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_JOB_EXPIRED).count()).isPositive();
    }

    private UUID insertWorkingOrder(String idemKey, String symbol, String qty, String limitPx, int version) {
        UUID id = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id
                        ) VALUES (
                          ?, ?, ?, 0, ?, 'WORKING', 'BUY', ?, CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'hash', NULL
                        )
                        """,
                id,
                accountId,
                idemKey,
                version,
                symbol,
                qty,
                limitPx);
        return id;
    }
}
