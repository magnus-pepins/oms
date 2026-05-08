package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.routing.RouteDispatcher;
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
 * Slice 5 prep: {@code oms.fix.symbol-map-json} maps OMS {@code instrument_symbol} to broker {@code Symbol} on NOS;
 * DB row keeps the client symbol.
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixRoundTripSymbolMapSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private RouteDispatcher routeDispatcher;

    @Autowired
    private JdbcTemplate jdbc;

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
        registry.add("oms.fix.symbol-map-json", () -> "{\"AAPL\":\"BRK.A\"}");
    }

    @BeforeEach
    void truncateOrders() {
        FixRoundTripAcceptorApplication.resetItHooks();
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void enqueueWorkingOrder_mapsSymbolOnWire_dbKeepsClientSymbol() {
        UUID orderId = insertWorkingOrder("fix-symmap-1", "AAPL", "10", "5.00", 1);
        routeDispatcher.enqueueWorkingOrder(orderId);

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("FILLED"));

        assertThat(jdbc.queryForObject("SELECT instrument_symbol FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("AAPL");
        assertThat(jdbc.queryForObject("SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId))
                .isEqualByComparingTo("10");
        assertThat(FixRoundTripAcceptorApplication.LAST_NOS_SYMBOL.get()).isEqualTo("BRK.A");
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
