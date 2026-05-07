package com.balh.oms.returnpath;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.domain.RejectCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Venue/broker reject applies {@code REJECT} execution row + {@code OrderRejected} fanout (slice 4).
 */
class VenueRejectReturnPathIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ExecutionReportApplier executionReportApplier;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void truncate() {
        jdbc.update("TRUNCATE TABLE orders CASCADE");
    }

    @Test
    void venueRejectApplied_insertsExecutionAndOrderRejected() {
        UUID orderId = insertWorkingOrder("vr-1", "AAPL", "10", "5.00", 1);
        var cmd = new ExecutionVenueRejectCommand(
                orderId,
                "FIX",
                Instant.parse("2026-05-07T12:00:00Z"),
                "venue-rej-1",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"REJECTED\"}");

        assertThat(executionReportApplier.applyVenueReject(cmd, RejectCode.VENUE_REJECT))
                .isEqualTo(ExecutionReportApplier.VenueRejectApplyOutcome.APPLIED);

        assertThat(jdbc.queryForObject("SELECT status::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("REJECTED");
        assertThat(jdbc.queryForObject(
                        "SELECT terminal_reason::text FROM orders WHERE id = ?", String.class, orderId))
                .isEqualTo("VENUE_REJECT");

        assertThat(jdbc.queryForObject(
                        "SELECT exec_type::text FROM executions WHERE order_id = ?",
                        String.class,
                        orderId))
                .isEqualTo("REJECT");

        List<String> types = jdbc.query(
                "SELECT envelope_json->>'type' AS t FROM domain_event_outbox WHERE order_id = ? ORDER BY id",
                (rs, i) -> rs.getString("t"),
                orderId);
        assertThat(types).contains("OrderRejected");
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
