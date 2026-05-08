package com.balh.oms.persistence;

import com.balh.oms.settlement.SettlementExecutionRow;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists applied venue execution reports (slice 3).
 */
@Repository
public class ExecutionsRepository {

    private static final String INSERT_TRADE_SQL = """
            INSERT INTO executions (
                order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                last_quantity, last_price, leaves_quantity, cum_quantity_after,
                exec_type, raw_envelope_json
            ) VALUES (
                :order_id, :account_id, :venue_id, :venue_ts, :venue_exec_ref,
                :last_quantity, :last_price, :leaves_quantity, :cum_quantity_after,
                CAST(:exec_type AS execution_exec_type), CAST(:raw_json AS JSONB)
            )
            ON CONFLICT (account_id, venue_exec_ref) DO NOTHING
            RETURNING id
            """;

    private static final String WEIGHTED_AVG_PRICE_SQL = """
            SELECT COALESCE(
                SUM(last_quantity * last_price) / NULLIF(SUM(last_quantity), 0),
                0
            )
            FROM executions
            WHERE order_id = :order_id AND exec_type = CAST('TRADE' AS execution_exec_type)
              AND last_price IS NOT NULL
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public ExecutionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public BigDecimal weightedAverageTradePrice(UUID orderId) {
        BigDecimal v = jdbc.queryForObject(
                WEIGHTED_AVG_PRICE_SQL,
                new MapSqlParameterSource("order_id", orderId),
                BigDecimal.class);
        return v == null ? BigDecimal.ZERO : v;
    }

    /**
     * @return inserted {@code executions.id}, or empty if idempotent duplicate on {@code (account_id, venue_exec_ref)}.
     */
    public Optional<Long> tryInsertTrade(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal lastQuantity,
            BigDecimal lastPrice,
            BigDecimal leavesQuantity,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(INSERT_TRADE_SQL, orderId, accountId, venueId, venueTs, venueExecRef,
                lastQuantity, lastPrice, leavesQuantity, cumQuantityAfter, "TRADE", rawJson);
    }

    /**
     * Cancel acknowledgement row (audit); idempotent on {@code (account_id, venue_exec_ref)}.
     */
    public Optional<Long> tryInsertCancel(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(INSERT_TRADE_SQL, orderId, accountId, venueId, venueTs, venueExecRef,
                BigDecimal.ZERO, null, BigDecimal.ZERO, cumQuantityAfter, "CANCEL", rawJson);
    }

    /**
     * Venue/broker reject audit row; idempotent on {@code (account_id, venue_exec_ref)}.
     */
    public Optional<Long> tryInsertVenueReject(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(INSERT_TRADE_SQL, orderId, accountId, venueId, venueTs, venueExecRef,
                BigDecimal.ZERO, null, BigDecimal.ZERO, cumQuantityAfter, "REJECT", rawJson);
    }

    private Optional<Long> insertReturning(
            String sql,
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal lastQuantity,
            BigDecimal lastPrice,
            BigDecimal leavesQuantity,
            BigDecimal cumQuantityAfter,
            String execType,
            String rawJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("account_id", accountId)
                .addValue("venue_id", venueId)
                .addValue("venue_ts", java.sql.Timestamp.from(venueTs))
                .addValue("venue_exec_ref", venueExecRef)
                .addValue("last_quantity", lastQuantity)
                .addValue("last_price", lastPrice)
                .addValue("leaves_quantity", leavesQuantity)
                .addValue("cum_quantity_after", cumQuantityAfter)
                .addValue("exec_type", execType)
                .addValue("raw_json", rawJson);
        List<Long> ids = jdbc.query(
                sql,
                params,
                (rs, rowNum) -> rs.getLong("id"));
        return ids.isEmpty() ? Optional.empty() : Optional.of(ids.getFirst());
    }

    private static final String SELECT_TRADE_SETTLEMENT_STATUSES_FOR_ORDER = """
            SELECT settlement_status::text AS settlement_status
            FROM executions
            WHERE order_id = :order_id
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            """;

    /**
     * Settlement status strings for {@code TRADE} rows only (one row per partial fill / execution).
     */
    public List<String> listTradeSettlementStatusesForOrder(UUID orderId) {
        return jdbc.query(
                SELECT_TRADE_SETTLEMENT_STATUSES_FOR_ORDER,
                new MapSqlParameterSource("order_id", orderId),
                (rs, rowNum) -> rs.getString("settlement_status"));
    }

    private static final String SELECT_SETTLEMENT_ROW = """
            SELECT e.id AS execution_id, e.settlement_status::text AS settlement_status, e.exec_type::text AS exec_type,
                   e.last_quantity, e.account_id, o.instrument_symbol, o.side::text AS side,
                   e.sell_position_from_pending_buy, e.sell_position_from_settled
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE e.id = :id
            """;

    private static final String UPDATE_SETTLEMENT_STATUS_IF = """
            UPDATE executions SET settlement_status = CAST(:to_status AS execution_settlement_status)
            WHERE id = :id
              AND settlement_status = CAST(:from_status AS execution_settlement_status)
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            """;

    public Optional<SettlementExecutionRow> findSettlementRow(long executionId) {
        var params = new MapSqlParameterSource("id", executionId);
        List<SettlementExecutionRow> rows = jdbc.query(
                SELECT_SETTLEMENT_ROW,
                params,
                (rs, rowNum) -> new SettlementExecutionRow(
                        rs.getLong("execution_id"),
                        rs.getString("settlement_status"),
                        rs.getString("exec_type"),
                        rs.getBigDecimal("last_quantity"),
                        rs.getObject("account_id", UUID.class),
                        rs.getString("instrument_symbol"),
                        rs.getString("side"),
                        rs.getBigDecimal("sell_position_from_pending_buy"),
                        rs.getBigDecimal("sell_position_from_settled")));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    /**
     * CAS settlement status forward for a {@code TRADE} row.
     *
     * @return rows updated (0 or 1).
     */
    public int updateSettlementStatusIf(long executionId, String fromStatus, String toStatus) {
        return jdbc.update(
                UPDATE_SETTLEMENT_STATUS_IF,
                new MapSqlParameterSource()
                        .addValue("id", executionId)
                        .addValue("from_status", fromStatus)
                        .addValue("to_status", toStatus));
    }

    private static final String SELECT_ID_BY_ACCOUNT_VENUE_REF = """
            SELECT id FROM executions
            WHERE account_id = :account_id AND venue_exec_ref = :venue_ref
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            LIMIT 1
            """;

    private static final String FAIL_TRADE_SETTLEMENT_SQL = """
            UPDATE executions SET settlement_status = CAST('failed' AS execution_settlement_status)
            WHERE id = :id
              AND exec_type = CAST('TRADE' AS execution_exec_type)
              AND settlement_status <> CAST('settled' AS execution_settlement_status)
              AND settlement_status <> CAST('failed' AS execution_settlement_status)
            """;

    public Optional<Long> findTradeExecutionIdByAccountAndVenueRef(UUID accountId, String venueExecRef) {
        if (accountId == null || venueExecRef == null || venueExecRef.isBlank()) {
            return Optional.empty();
        }
        List<Long> ids = jdbc.query(
                SELECT_ID_BY_ACCOUNT_VENUE_REF,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("venue_ref", venueExecRef.trim()),
                (rs, rowNum) -> rs.getLong("id"));
        return ids.isEmpty() ? Optional.empty() : Optional.of(ids.getFirst());
    }

    /**
     * Moves a {@code TRADE} execution to {@code failed} unless already {@code settled} or {@code failed}.
     *
     * @return rows updated (0 or 1).
     */
    public int failTradeSettlementToFailed(long executionId) {
        return jdbc.update(
                FAIL_TRADE_SETTLEMENT_SQL, new MapSqlParameterSource("id", executionId));
    }

    private static final String UPDATE_SELL_FILL_POSITION_SPLIT_SQL = """
            UPDATE executions SET
                sell_position_from_pending_buy = :from_pb,
                sell_position_from_settled = :from_settled
            WHERE id = :id AND exec_type = CAST('TRADE' AS execution_exec_type)
            """;

    /**
     * Persists how a SELL fill was sourced from {@code positions} (for exact unwind on mark-failed).
     */
    public void updateSellFillPositionSplit(long executionId, SellFillPositionSplit split) {
        int n = jdbc.update(
                UPDATE_SELL_FILL_POSITION_SPLIT_SQL,
                new MapSqlParameterSource()
                        .addValue("id", executionId)
                        .addValue("from_pb", split.fromPendingBuy())
                        .addValue("from_settled", split.fromSettled()));
        if (n != 1) {
            throw new IllegalStateException("update sell fill split: expected 1 row, got " + n + " execution=" + executionId);
        }
    }
}
