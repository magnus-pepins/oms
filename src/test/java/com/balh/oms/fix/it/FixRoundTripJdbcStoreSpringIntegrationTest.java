package com.balh.oms.fix.it;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.fix.FixJdbcSessionSchema;
import com.balh.oms.fix.FixMetrics;
import com.balh.oms.routing.RouteDispatcher;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import com.balh.oms.test.SettlementBrokerDrainAssertions;
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
 * Same as {@link FixRoundTripSpringIntegrationTest} but {@code oms.fix.session-store-type=jdbc} so QuickFIX/J persists
 * seq/messages in {@code oms_fix_sessions} / {@code oms_fix_messages} (Flyway V9).
 */
@Import(FixRoundTripTestBeans.class)
@ActiveProfiles({"test", "fix-roundtrip-it"})
class FixRoundTripJdbcStoreSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private RouteDispatcher routeDispatcher;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private SettlementConfirmProcessor settlementConfirmProcessor;

    @DynamicPropertySource
    static void fixRoundTripProps(DynamicPropertyRegistry registry) {
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.session-store-type", () -> "jdbc");
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        FixRoundTripDynamicProperties.registerLoopbackPort(registry);
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
        // QuickFIX/J's JdbcStore writes the session row at QuickFIX session creation
        // (Spring context init). It only INSERTs once and UPDATEs thereafter, so truncating
        // oms_fix_sessions/oms_fix_messages here would leave subsequent UPDATEs affecting
        // zero rows, breaking the COUNT > 0 assertion despite a healthy round-trip. We rely
        // on per-test-class Spring contexts for isolation instead.
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void jdbcStore_roundTrip_writesSessionRow() {
        UUID orderId = insertWorkingOrder("fix-jdbc-store-1", "AAPL", "10", "5.00", 1);
        routeDispatcher.enqueueWorkingOrder(orderId);

        await().atMost(Duration.ofSeconds(45))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(
                                jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                        .isEqualTo("FILLED"));

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + FixJdbcSessionSchema.SESSIONS_TABLE, Long.class))
                .isPositive();
        assertThat(meterRegistry.counter(FixMetrics.METRIC_NOS_SENT).count()).isPositive();

        SettlementBrokerDrainAssertions.assertFullBrokerLifecycleSettles(
                jdbc, settlementConfirmProcessor, orderId, 1, new BigDecimal("10"));
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
