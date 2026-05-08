package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.fix.FixJdbcSessionSchema;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code oms.fix.session-jdbc-datasource-enabled=true} with a second Hikari pool (here: same Postgres as IT for
 * wiring proof); QuickFIX {@code JdbcStoreFactory} must still complete a round-trip.
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixRoundTripJdbcDedicatedSessionPoolSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private RouteDispatcher routeDispatcher;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @DynamicPropertySource
    static void fixRoundTripProps(DynamicPropertyRegistry registry) {
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.session-store-type", () -> "jdbc");
        registry.add("oms.fix.session-jdbc-datasource-enabled", () -> "true");
        registry.add("oms.fix.session-jdbc-url", POSTGRES::getJdbcUrl);
        registry.add("oms.fix.session-jdbc-user", POSTGRES::getUsername);
        registry.add("oms.fix.session-jdbc-password", POSTGRES::getPassword);
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
    void truncate() {
        FixRoundTripAcceptorApplication.resetItHooks();
        jdbc.update("TRUNCATE TABLE " + FixJdbcSessionSchema.MESSAGES_TABLE);
        jdbc.update("TRUNCATE TABLE " + FixJdbcSessionSchema.SESSIONS_TABLE);
        jdbc.update("TRUNCATE TABLE orders CASCADE");
    }

    @Test
    void dedicatedSessionPool_roundTrip_writesSessionRow() {
        UUID orderId = insertWorkingOrder("fix-jdbc-dedicated-1", "AAPL", "10", "5.00", 1);
        routeDispatcher.enqueueWorkingOrder(orderId);

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("FILLED"));

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + FixJdbcSessionSchema.SESSIONS_TABLE, Long.class))
                .isPositive();
        assertThat(meterRegistry.counter(FixMetrics.METRIC_NOS_SENT).count()).isPositive();
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
