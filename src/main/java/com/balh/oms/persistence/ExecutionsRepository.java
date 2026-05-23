package com.balh.oms.persistence;

import com.balh.oms.settlement.SettlementExecutionRow;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
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
                exec_type, raw_envelope_json,
                trade_date, expected_settlement_date
            ) VALUES (
                :order_id, :account_id, :venue_id, :venue_ts, :venue_exec_ref,
                :last_quantity, :last_price, :leaves_quantity, :cum_quantity_after,
                CAST(:exec_type AS execution_exec_type), CAST(:raw_json AS JSONB),
                :trade_date, :expected_settlement_date
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
     * @param tradeDate calendar date of the execution; populated by
     *                  {@link com.balh.oms.settlement.SettlementDateCalculator#computeTradeDate}.
     *                  Nullable for historical / backfill writers that do not have it; new
     *                  callers (gap plan §5.3 Slice 1) must pass it.
     * @param expectedSettlementDate OMS-computed expected settlement date (see
     *                  {@link com.balh.oms.settlement.SettlementDateCalculator#computeExpectedSettlementDate}).
     *                  Nullable for the same reason.
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
            String rawJson,
            LocalDate tradeDate,
            LocalDate expectedSettlementDate) {
        return insertReturning(INSERT_TRADE_SQL, orderId, accountId, venueId, venueTs, venueExecRef,
                lastQuantity, lastPrice, leavesQuantity, cumQuantityAfter, "TRADE", rawJson,
                tradeDate, expectedSettlementDate);
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
                BigDecimal.ZERO, null, BigDecimal.ZERO, cumQuantityAfter, "CANCEL", rawJson,
                /* tradeDate = */ null, /* expectedSettlementDate = */ null);
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
                BigDecimal.ZERO, null, BigDecimal.ZERO, cumQuantityAfter, "REJECT", rawJson,
                /* tradeDate = */ null, /* expectedSettlementDate = */ null);
    }

    /**
     * Venue/broker REPLACE acknowledgement audit row. The broker sent us ER 35=5 in response to
     * a 35=G we issued; the row records the broker-authoritative new total quantity and limit
     * price (see {@code ApplyExecutionReportCommand.EXEC_TYPE_REPLACE} doc for the
     * {@code lastQtyScaled}/{@code lastPxScaled} reinterpretation). {@code newQuantity} maps to
     * {@code last_quantity}, {@code newLimitPriceOrNull} maps to {@code last_price}; cumQty after
     * replace is unchanged ({@code cumQuantityAfter} carries the pre-replace cumulative). Leaves
     * quantity for a replace is the new total minus the unchanged cumQty — useful when the
     * settlement / desk side reconstructs leaves from the executions log without re-reading the
     * orders row.
     *
     * <p>Idempotent on {@code (account_id, venue_exec_ref)} like the trade / cancel / reject
     * inserts.
     */
    public Optional<Long> tryInsertReplace(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal newQuantity,
            BigDecimal newLimitPriceOrNull,
            BigDecimal leavesQuantity,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(
                INSERT_TRADE_SQL,
                orderId,
                accountId,
                venueId,
                venueTs,
                venueExecRef,
                newQuantity,
                newLimitPriceOrNull,
                leavesQuantity,
                cumQuantityAfter,
                "REPLACE",
                rawJson,
                /* tradeDate = */ null,
                /* expectedSettlementDate = */ null);
    }

    /**
     * 35=9 OrderCancelReject (against a prior 35=F cancel). No state change to the order on the
     * venue's books and none in our cluster; this row is the audit trail for "broker said no" so
     * downstream surfaces (UI toast, ops alerts) can reconstruct the rejection without a
     * back-channel to the venue.
     */
    public Optional<Long> tryInsertCancelReject(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(
                INSERT_TRADE_SQL,
                orderId,
                accountId,
                venueId,
                venueTs,
                venueExecRef,
                BigDecimal.ZERO,
                null,
                BigDecimal.ZERO,
                cumQuantityAfter,
                "CANCEL_REJECT",
                rawJson,
                /* tradeDate = */ null,
                /* expectedSettlementDate = */ null);
    }

    /**
     * 35=9 OrderCancelReject (against a prior 35=G replace). Mirrors {@link #tryInsertCancelReject}
     * except for the exec_type discriminator; OMS distinguishes them so downstream can route the
     * toast text correctly ("cancel rejected" vs. "modify rejected").
     */
    public Optional<Long> tryInsertReplaceReject(
            UUID orderId,
            UUID accountId,
            String venueId,
            Instant venueTs,
            String venueExecRef,
            BigDecimal cumQuantityAfter,
            String rawJson) {
        return insertReturning(
                INSERT_TRADE_SQL,
                orderId,
                accountId,
                venueId,
                venueTs,
                venueExecRef,
                BigDecimal.ZERO,
                null,
                BigDecimal.ZERO,
                cumQuantityAfter,
                "REPLACE_REJECT",
                rawJson,
                /* tradeDate = */ null,
                /* expectedSettlementDate = */ null);
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
            String rawJson,
            LocalDate tradeDate,
            LocalDate expectedSettlementDate) {
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
                .addValue("raw_json", rawJson)
                .addValue("trade_date", tradeDate == null ? null : java.sql.Date.valueOf(tradeDate))
                .addValue(
                        "expected_settlement_date",
                        expectedSettlementDate == null ? null : java.sql.Date.valueOf(expectedSettlementDate));
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
            SELECT e.id AS execution_id, e.order_id, e.settlement_status::text AS settlement_status, e.exec_type::text AS exec_type,
                   e.last_quantity, e.last_price, e.account_id, o.instrument_symbol, o.side::text AS side,
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
                        rs.getObject("order_id", UUID.class),
                        rs.getString("settlement_status"),
                        rs.getString("exec_type"),
                        rs.getBigDecimal("last_quantity"),
                        rs.getBigDecimal("last_price"),
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

    private static final String SELECT_EXPECTED_SETTLEMENT_DATE = """
            SELECT expected_settlement_date
            FROM executions
            WHERE id = :id
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            """;

    /**
     * Returns the OMS-computed expected settlement date stored at TRADE-projection time
     * (gap plan §5.3 Slice 1 / V58). Empty when the execution row is absent, is not a TRADE,
     * or pre-dates V58 (NULL column). Callers must treat empty as "OMS expectation unknown,
     * do not raise a calendar-drift break".
     */
    public Optional<LocalDate> findExpectedSettlementDate(long executionId) {
        List<LocalDate> dates = jdbc.query(
                SELECT_EXPECTED_SETTLEMENT_DATE,
                new MapSqlParameterSource("id", executionId),
                (rs, rowNum) -> {
                    java.sql.Date d = rs.getDate("expected_settlement_date");
                    return d == null ? null : d.toLocalDate();
                });
        if (dates.isEmpty() || dates.getFirst() == null) {
            return Optional.empty();
        }
        return Optional.of(dates.getFirst());
    }

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

    private static final String INCREMENT_AUTO_STEP_FAILURES_SQL = """
            UPDATE executions
            SET settlement_auto_step_failures = settlement_auto_step_failures + 1,
                settlement_auto_step_last_failure_at = :ts,
                settlement_auto_step_last_error = :err
            WHERE id = :id
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            RETURNING settlement_auto_step_failures
            """;

    private static final String CLEAR_AUTO_STEP_FAILURES_SQL = """
            UPDATE executions
            SET settlement_auto_step_failures = 0,
                settlement_auto_step_last_failure_at = NULL,
                settlement_auto_step_last_error = NULL
            WHERE id = :id
            """;

    /**
     * Records one {@link com.balh.oms.settlement.SettlementAutoStepScheduler} advance failure.
     *
     * @return new failure count after increment, or empty if the execution id is missing / not TRADE
     */
    public Optional<Integer> recordSettlementAutoStepFailure(long executionId, String errorText) {
        String err = errorText;
        if (err != null && err.length() > 500) {
            err = err.substring(0, 500);
        }
        List<Integer> counts = jdbc.query(
                INCREMENT_AUTO_STEP_FAILURES_SQL,
                new MapSqlParameterSource()
                        .addValue("id", executionId)
                        .addValue("ts", java.sql.Timestamp.from(java.time.Instant.now()))
                        .addValue("err", err),
                (rs, rowNum) -> rs.getInt("settlement_auto_step_failures"));
        return counts.isEmpty() ? Optional.empty() : Optional.of(counts.getFirst());
    }

    /** Clears auto-step failure state after a successful advance. */
    public void clearSettlementAutoStepFailures(long executionId) {
        jdbc.update(CLEAR_AUTO_STEP_FAILURES_SQL, new MapSqlParameterSource("id", executionId));
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

    private static final String SELECT_UNSETTLED_BUY_TRADE_IDS_FOR_ATTRIBUTION = """
            SELECT e.id
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE e.account_id = :account_id
              AND o.instrument_symbol = :symbol
              AND o.side = CAST('BUY' AS order_side)
              AND e.id <> :exclude_id
              AND e.exec_type = CAST('TRADE' AS execution_exec_type)
              AND e.settlement_status::text IN ('executed', 'matched')
            ORDER BY e.id DESC
            LIMIT :limit
            """;

    private static final String APPEND_UNSETTLED_FUNDED_ONE = """
            UPDATE executions
            SET unsettled_funded_by_exec_ids = unsettled_funded_by_exec_ids || ARRAY[ :fid ]::bigint[]
            WHERE id = :eid
              AND exec_type = CAST('TRADE' AS execution_exec_type)
            """;

    /**
     * Prior BUY trade legs for the same account/symbol still in early settlement states (stub free-riding input).
     */
    public List<Long> findUnsettledBuyTradeExecutionIdsForAttribution(
            UUID accountId, String instrumentSymbol, long excludeExecutionId, int limit) {
        return jdbc.query(
                SELECT_UNSETTLED_BUY_TRADE_IDS_FOR_ATTRIBUTION,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", instrumentSymbol)
                        .addValue("exclude_id", excludeExecutionId)
                        .addValue("limit", limit),
                (rs, rowNum) -> rs.getLong("id"));
    }

    public void appendUnsettledFundedByExecutionIds(long executionId, List<Long> fundingExecutionIds) {
        if (fundingExecutionIds.isEmpty()) {
            return;
        }
        for (Long fid : fundingExecutionIds) {
            int n = jdbc.update(
                    APPEND_UNSETTLED_FUNDED_ONE,
                    new MapSqlParameterSource().addValue("fid", fid).addValue("eid", executionId));
            if (n != 1) {
                throw new IllegalStateException(
                        "append unsettled_funded_by_exec_ids: expected 1 row, got " + n + " execution=" + executionId);
            }
        }
    }
}
