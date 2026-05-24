package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for V62's backfill of {@code executions.trade_date} on pre-V58 TRADE
 * rows (gap plan §5.3 Slice 2b-2).
 *
 * <p>V62 has already run inside the shared Testcontainers Postgres by the time these tests
 * execute, so the contract under test is "after V62 + after my own simulated legacy row,
 * a manual re-run of V62's UPDATE statement on the same legacy row produces the right
 * trade_date." We can't replay V62 itself per-test (Flyway treats it as already-applied),
 * but the SQL is deterministic and identical to what V62 issues — re-running it here is
 * the closest evidence we can give that the migration behaves correctly for any rows that
 * sneak into the legacy shape via future replay or restore.
 */
class ExecutionsTradeDateBackfillIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    JdbcTemplate jdbc;

    /** Identical to V62's UPDATE. Kept in sync via this test — see V62 comment block. */
    private static final String BACKFILL_SQL =
            """
                    UPDATE executions
                    SET trade_date = (venue_ts AT TIME ZONE 'UTC')::date
                    WHERE exec_type = 'TRADE'::execution_exec_type
                      AND trade_date IS NULL
                      AND venue_ts IS NOT NULL
                    """;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void backfill_populatesTradeDateFromVenueTsUtcOnLegacyTradeRow() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        seedOrder(orderId, accountId);
        // Venue ts at 13:45 UTC on 2026-05-20 → trade_date 2026-05-20 (Wednesday).
        Long execId = seedLegacyTradeExecution(
                orderId, accountId, "EX-BACKFILL-1", "2026-05-20T13:45:00Z");

        int updated = jdbc.update(BACKFILL_SQL);

        assertThat(updated).isEqualTo(1);
        LocalDate tradeDate = jdbc.queryForObject(
                "SELECT trade_date FROM executions WHERE id = ?",
                (rs, rowNum) -> {
                    Date d = rs.getDate("trade_date");
                    return d == null ? null : d.toLocalDate();
                },
                execId);
        assertThat(tradeDate).isEqualTo(LocalDate.of(2026, 5, 20));
        // expected_settlement_date stays NULL by design — V62 does NOT backfill it.
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT expected_settlement_date FROM executions WHERE id = ?", execId);
        assertThat(row.get("expected_settlement_date")).isNull();
    }

    @Test
    void backfill_picksUtcCalendarDateForLateEveningVenueTs() {
        // 23:30 UTC on 2026-05-20 → trade_date 2026-05-20 (still Wednesday in UTC).
        UUID orderId = UUID.randomUUID();
        seedOrder(orderId, UUID.randomUUID());
        Long execId = seedLegacyTradeExecution(
                orderId, UUID.randomUUID(), "EX-BACKFILL-LATE", "2026-05-20T23:30:00Z");

        jdbc.update(BACKFILL_SQL);

        LocalDate tradeDate = jdbc.queryForObject(
                "SELECT trade_date FROM executions WHERE id = ?",
                (rs, rowNum) -> rs.getDate("trade_date").toLocalDate(),
                execId);
        assertThat(tradeDate).isEqualTo(LocalDate.of(2026, 5, 20));
    }

    @Test
    void backfill_isIdempotent_doesNotOverwriteAlreadyPopulatedRows() {
        // Mirrors what happens on a re-run: V58-and-later TRADE rows already carry
        // trade_date; the backfill must leave them alone.
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        seedOrder(orderId, accountId);
        Long execId = seedTradeExecutionWithDates(
                orderId, accountId, "EX-ALREADY-SET", "2026-05-20T13:45:00Z",
                LocalDate.of(2026, 5, 21) /* deliberately different from venue_ts UTC date */);

        int updated = jdbc.update(BACKFILL_SQL);

        assertThat(updated).isZero();
        LocalDate tradeDate = jdbc.queryForObject(
                "SELECT trade_date FROM executions WHERE id = ?",
                (rs, rowNum) -> rs.getDate("trade_date").toLocalDate(),
                execId);
        assertThat(tradeDate).isEqualTo(LocalDate.of(2026, 5, 21)); // unchanged
    }

    @Test
    void backfill_skipsNonTradeExecutions() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        seedOrder(orderId, accountId);
        // A non-TRADE row (e.g. ORDER_ACCEPTED / CANCEL_APPLIED) carries no trade_date by
        // design. The backfill must not invent one.
        Long execId = seedLegacyNonTradeExecution(orderId, accountId);

        int updated = jdbc.update(BACKFILL_SQL);

        assertThat(updated).isZero();
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT trade_date FROM executions WHERE id = ?", execId);
        assertThat(row.get("trade_date")).isNull();
    }

    private void seedOrder(UUID orderId, UUID accountId) {
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', CAST('BUY' AS order_side), 'AAPL',
                          CAST('10' AS NUMERIC), CAST('5.00' AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'h', NULL, CAST('10' AS NUMERIC)
                        )
                        """,
                orderId,
                accountId,
                "backfill-" + orderId);
    }

    private Long seedLegacyTradeExecution(UUID orderId, UUID accountId, String venueExecRef, String venueTsIso) {
        return jdbc.queryForObject(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', CAST(? AS TIMESTAMPTZ), ?,
                          CAST('10' AS NUMERIC), CAST('5.00' AS NUMERIC), 0, CAST('10' AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        RETURNING id
                        """,
                Long.class,
                orderId,
                accountId,
                venueTsIso,
                venueExecRef);
    }

    private Long seedTradeExecutionWithDates(
            UUID orderId, UUID accountId, String venueExecRef, String venueTsIso, LocalDate tradeDate) {
        return jdbc.queryForObject(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, trade_date, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'SIM', CAST(? AS TIMESTAMPTZ), ?,
                          CAST('10' AS NUMERIC), CAST('5.00' AS NUMERIC), 0, CAST('10' AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          ?, NULL
                        )
                        RETURNING id
                        """,
                Long.class,
                orderId,
                accountId,
                venueTsIso,
                venueExecRef,
                Date.valueOf(tradeDate));
    }

    private Long seedLegacyNonTradeExecution(UUID orderId, UUID accountId) {
        // The execution_exec_type enum (V6) only carries TRADE / CANCEL. CANCEL rows have
        // no business-meaningful trade_date — confirms the backfill leaves them alone.
        return jdbc.queryForObject(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          CAST('0' AS NUMERIC), CAST('0' AS NUMERIC), 0, CAST('0' AS NUMERIC),
                          CAST('CANCEL' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        RETURNING id
                        """,
                Long.class,
                orderId,
                accountId,
                "EX-NON-TRADE-" + UUID.randomUUID());
    }
}
