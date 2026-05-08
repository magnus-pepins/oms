package com.balh.oms.tailer;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.chronicle.PendingControlEvent;
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
 * Slice 5 v1: configured tradable universe rejects with {@code RISK_INSTRUMENT_NOT_ALLOWED}
 * (config list until marketdata-backed {@code instruments} cache exists).
 */
class InstrumentTradabilityControlIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired ControlTailer controlTailer;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void truncateTradingTables() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        jdbc.update("TRUNCATE TABLE oms_runtime_flags RESTART IDENTITY");
    }

    @DynamicPropertySource
    static void tradabilityProps(DynamicPropertyRegistry registry) {
        registry.add("oms.risk.instrument-allowlist-enabled", () -> "false");
        registry.add("oms.risk.instrument-tradability-check-enabled", () -> "true");
        registry.add("oms.risk.tradable-instrument-symbols", () -> "AAPL");
    }

    @Test
    void nonTradableSymbolRejectedWithInstrumentNotAllowed() {
        UUID orderId = insertOrderRow("trad-1", "MSFT", "1", "10.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.RISK_PIPELINE_REJECTED);
        assertThat(jdbc.queryForObject(
                        "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("RISK_INSTRUMENT_NOT_ALLOWED");
    }

    @Test
    void tradableSymbolPassesRiskToWorking() {
        UUID orderId = insertOrderRow("trad-2", "AAPL", "1", "10.00");
        PendingControlEvent ev = event(orderId, 0);
        assertThat(controlTailer.apply(ev)).isEqualTo(ControlTailer.TailResult.APPLIED);
        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("WORKING");
    }

    private static PendingControlEvent event(UUID orderId, int version) {
        Instant ts = Instant.parse("2026-05-07T14:00:00Z");
        return new PendingControlEvent("OrderAccepted", orderId, version, 0, "h", ts, ts);
    }

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
