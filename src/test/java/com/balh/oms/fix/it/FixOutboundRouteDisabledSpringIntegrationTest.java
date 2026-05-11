package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.fix.FixMetrics;
import com.balh.oms.routing.RouteDispatcher;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
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
 * When {@code fix_route_state.send_enabled} is false, the outbound worker must not dequeue — no NOS and order stays
 * {@code WORKING}; metric {@code oms_fix_outbound_route_disabled_skips_total} increments. Re-enable matches round-trip.
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixOutboundRouteDisabledSpringIntegrationTest extends AbstractPostgresIntegrationTest {

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
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        FixRoundTripDynamicProperties.registerLoopbackPort(registry);
        registry.add(
                "oms.fix.file-store-path",
                () -> FixRoundTripFixture.INITIATOR_STORE.toAbsolutePath().toString());
        registry.add("oms.fix.sender-comp-id", () -> "INITIATOR");
        registry.add("oms.fix.target-comp-id", () -> "ACCEPTOR");
        registry.add("oms.fix.outbound-poll-interval-ms", () -> "25");
        registry.add("oms.fix.outbound-driver", () -> "dedicated");
        registry.add("oms.fix.venue-id-for-executions", () -> "FIX_IT");
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "false");
    }

    @BeforeEach
    void reset() {
        FixRoundTripAcceptorApplication.resetItHooks();
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        jdbc.update("UPDATE fix_route_state SET send_enabled = TRUE, updated_by = 'test', note = NULL WHERE route_key = 'default'");
    }

    @AfterEach
    void restoreRoute() {
        jdbc.update("UPDATE fix_route_state SET send_enabled = TRUE, updated_by = 'test', note = NULL WHERE route_key = 'default'");
    }

    @Test
    void routeDisabled_blocksNos_thenReEnableFills() {
        jdbc.update("UPDATE fix_route_state SET send_enabled = FALSE WHERE route_key = 'default'");

        UUID orderId = insertWorkingOrder("fix-route-off", "AAPL", "10", "5.00", 1);
        routeDispatcher.enqueueWorkingOrder(orderId);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).count())
                        .isPositive());

        assertThat(FixRoundTripAcceptorApplication.NOS_RECEIVED.get()).isZero();
        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("WORKING");

        jdbc.update("UPDATE fix_route_state SET send_enabled = TRUE WHERE route_key = 'default'");

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("FILLED"));

        assertThat(FixRoundTripAcceptorApplication.NOS_RECEIVED.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId))
                .isEqualByComparingTo("10");
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
