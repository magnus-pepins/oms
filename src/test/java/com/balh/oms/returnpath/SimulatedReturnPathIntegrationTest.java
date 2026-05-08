package com.balh.oms.returnpath;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.routing.SimulatedReturnPathProjectionWorker;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import com.balh.oms.tailer.ControlTailer;
import com.balh.oms.test.SettlementBrokerDrainAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 3: simulated broker return path + {@code executions} + fill/cancel domain events.
 */
class SimulatedReturnPathIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired ControlTailer controlTailer;
    @Autowired ExecutionReportApplier executionReportApplier;
    @Autowired SimulatedReturnPathProjectionWorker simulatedReturnPathProjectionWorker;
    @Autowired JdbcTemplate jdbc;
    @Autowired SettlementConfirmProcessor settlementConfirmProcessor;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @DynamicPropertySource
    static void routingProps(DynamicPropertyRegistry registry) {
        registry.add("oms.routing.backend", () -> "simulated");
        registry.add("oms.routing.simulated.scheduler-enabled", () -> "false");
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "false");
    }

    @Test
    void controlPassThenSimulatedFillsProduceExecutionsAndFilledState() {
        UUID orderId = insertNewOrder("rp-1", "AAPL", "100", "10.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);

        simulatedReturnPathProjectionWorker.processPendingQueueOnce();

        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("FILLED");
        assertThat(jdbc.queryForObject("SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId))
                .isEqualByComparingTo("100");

        int execRows = jdbc.queryForObject("SELECT COUNT(*)::int FROM executions WHERE order_id = ?", Integer.class, orderId);
        assertThat(execRows).isEqualTo(3);

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM market_context WHERE order_id = ?", Integer.class, orderId)).isEqualTo(1);

        String expectedLastRef = "sim-" + orderId + "-3";
        assertThat(jdbc.queryForObject(
                        "SELECT snapshot_json->>'venueExecRef' FROM market_context WHERE order_id = ?",
                        String.class,
                        orderId))
                .isEqualTo(expectedLastRef);
        assertThat(jdbc.queryForObject(
                        "SELECT snapshot_json->>'instrumentSymbol' FROM market_context WHERE order_id = ?",
                        String.class,
                        orderId))
                .isEqualTo("AAPL");
        assertThat(jdbc.queryForObject(
                        "SELECT (snapshot_json->>'schemaVersion')::int FROM market_context WHERE order_id = ?",
                        Integer.class,
                        orderId))
                .isEqualTo(MarketContextVenueEvidence.SCHEMA_VERSION);
        assertThat(jdbc.queryForObject(
                        "SELECT snapshot_json->>'evidenceSource' FROM market_context WHERE order_id = ?",
                        String.class,
                        orderId))
                .isEqualTo("venue_execution_report");

        List<String> types = jdbc.query(
                "SELECT envelope_json->>'type' AS t FROM domain_event_outbox WHERE order_id = ? ORDER BY id",
                (rs, i) -> rs.getString("t"),
                orderId);
        assertThat(types).contains("OrderWorking", "OrderPartiallyFilled", "OrderFilled");

        UUID accountId = jdbc.queryForObject("SELECT account_id FROM orders WHERE id = ?", UUID.class, orderId);
        UUID custody = UUID.fromString("a0000001-0000-4000-8000-000000000001");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_total FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        BigDecimal.class,
                        accountId,
                        custody))
                .isEqualByComparingTo("100");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        BigDecimal.class,
                        accountId,
                        custody))
                .isEqualByComparingTo("100");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'TRADE_BUY_FILL'",
                        Integer.class,
                        accountId))
                .isEqualTo(3);
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE order_id = ? ORDER BY id LIMIT 1",
                        String.class,
                        orderId))
                .isEqualTo("executed");

        SettlementBrokerDrainAssertions.assertFullBrokerLifecycleSettles(
                jdbc, settlementConfirmProcessor, orderId, 3, new BigDecimal("100"));
    }

    @Test
    void duplicateVenueExecRefIsIdempotent() {
        UUID orderId = insertWorkingOrder("idem-1", "AAPL", "10", "5.00", 1);
        String venue = "SIM";
        Instant ts = Instant.parse("2026-05-07T12:00:00Z");
        var cmd = new ExecutionTradeCommand(orderId, venue, ts, "dup-ref-1", new BigDecimal("3"), new BigDecimal("5"),
                new BigDecimal("7"), new BigDecimal("3"));
        assertThat(executionReportApplier.applyTrade(cmd)).isEqualTo(ExecutionReportApplier.TradeApplyOutcome.APPLIED);
        assertThat(executionReportApplier.applyTrade(cmd)).isEqualTo(ExecutionReportApplier.TradeApplyOutcome.DUPLICATE);

        assertThat(jdbc.queryForObject("SELECT COUNT(*)::int FROM executions WHERE venue_exec_ref = ?", Integer.class, "dup-ref-1"))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT version FROM orders WHERE id = ?", Integer.class, orderId)).isEqualTo(2);
        assertThat(jdbc.queryForObject(
                        "SELECT snapshot_json->>'venueExecRef' FROM market_context WHERE order_id = ?",
                        String.class,
                        orderId))
                .isEqualTo("dup-ref-1");
    }

    @Test
    void cancelExecutionReportClosesWorkingOrder() {
        UUID orderId = insertWorkingOrder("cxl-1", "MSFT", "5", "12.00", 1);
        var cmd = new ExecutionCancelCommand(orderId, "SIM", Instant.parse("2026-05-07T15:00:00Z"), "cxl-ref-1");
        assertThat(executionReportApplier.applyCancel(cmd)).isEqualTo(ExecutionReportApplier.CancelApplyOutcome.APPLIED);
        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("CANCELLED");
        List<String> types = jdbc.query(
                "SELECT envelope_json->>'type' AS t FROM domain_event_outbox WHERE order_id = ? ORDER BY id",
                (rs, i) -> rs.getString("t"),
                orderId);
        assertThat(types).contains("OrderCancelled");
    }

    private static PendingControlEvent event(UUID orderId, int version) {
        Instant ts = Instant.parse("2026-05-07T14:00:00Z");
        return new PendingControlEvent("OrderAccepted", orderId, version, 0, "h", ts, ts);
    }

    private UUID insertNewOrder(String idemKey, String symbol, String qty, String limitPx) {
        UUID id = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id
                        ) VALUES (
                          ?, ?, ?, 0, 0, 'NEW', 'BUY', ?, CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'hash', NULL
                        )
                        """,
                id,
                accountId,
                idemKey,
                symbol,
                qty,
                limitPx);
        return id;
    }

    /** Pre-advanced to WORKING with {@code version} (simulates post-control state). */
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
