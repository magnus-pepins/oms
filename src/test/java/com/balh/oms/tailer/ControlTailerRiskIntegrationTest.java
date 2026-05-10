package com.balh.oms.tailer;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 2 risk pipeline + {@code control_decisions} + {@code oms_runtime_flags} against real Postgres.
 */
class ControlTailerRiskIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired ControlTailer controlTailer;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry meterRegistry;

    @BeforeEach
    void truncateTradingTables() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        jdbc.update("TRUNCATE TABLE oms_runtime_flags RESTART IDENTITY");
    }

    @DynamicPropertySource
    static void riskProps(DynamicPropertyRegistry registry) {
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "true");
        registry.add("oms.risk.allowed-instrument-symbols", () -> "AAPL");
        registry.add("oms.risk.fat-finger-max-limit-price", () -> "50");
        registry.add("oms.risk.fat-finger-max-order-quantity", () -> "100");
        registry.add("oms.risk.max-order-notional", () -> "500");
    }

    @Test
    void globalHaltRejectsWithKillSwitchAndRecordsDecision() {
        jdbc.update(
                "INSERT INTO oms_runtime_flags (flag_key, value_boolean) VALUES (?, true) "
                        + "ON CONFLICT (flag_key) DO UPDATE SET value_boolean = true",
                "global_halt");

        UUID orderId = insertOrderRow("halt-1", "AAPL", "1", "10.00");
        PendingControlEvent ev = event(orderId, 0);
        var killSwitchCounter =
                meterRegistry.counter(
                        "oms_control_decisions_recorded_total",
                        "outcome",
                        "REJECT",
                        "reject_code",
                        "RISK_KILL_SWITCH");
        double beforeRecorded = killSwitchCounter.count();
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.RISK_PIPELINE_REJECTED);

        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("REJECTED");
        assertThat(jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("RISK_KILL_SWITCH");
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM control_decisions WHERE order_id = ? AND outcome = 'REJECT'",
                Integer.class,
                orderId)).isEqualTo(1);
        assertThat(killSwitchCounter.count()).isEqualTo(beforeRecorded + 1.0);
    }

    @Test
    void instrumentAllowlistRejectsUnknownSymbol() {
        UUID orderId = insertOrderRow("sym-1", "MSFT", "1", "10.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.RISK_PIPELINE_REJECTED);
        assertThat(jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("RISK_INVALID_INSTRUMENT");
    }

    @Test
    void fatFingerLimitPriceRejects() {
        UUID orderId = insertOrderRow("ff-1", "AAPL", "1", "99.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.RISK_PIPELINE_REJECTED);
        assertThat(jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("RISK_FAT_FINGER_PRICE");
    }

    @Test
    void notionalCapRejects() {
        // Fat-finger limit price is evaluated before notional; stay at or below max limit (50)
        // while qty × limit exceeds max notional (500).
        UUID orderId = insertOrderRow("not-1", "AAPL", "11", "50.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.RISK_PIPELINE_REJECTED);
        assertThat(jdbc.queryForObject(
                "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("RISK_NOTIONAL_CAP");
    }

    private PendingControlEvent event(UUID orderId, int version) {
        Instant ts = Instant.now();
        return new PendingControlEvent("OrderAccepted", orderId, version, 0, "h", ts, ts);
    }

    /**
     * Inserts a minimal {@code orders} row in NEW status (same shape as ingress would),
     * bypassing HTTP for speed. Matches {@code OrdersRepository} column set including
     * {@code ledger_balance_id} nullable.
     */
    private UUID insertOrderRow(String idemKey, String symbol, String qty, String limitPx) {
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
}
