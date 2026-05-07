package com.balh.oms.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
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
     * @return {@code true} if a new row was inserted; {@code false} if idempotent duplicate
     *         on {@code (account_id, venue_exec_ref)}.
     */
    public boolean tryInsertTrade(
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
    public boolean tryInsertCancel(
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
    public boolean tryInsertVenueReject(
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

    private boolean insertReturning(
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
        return !ids.isEmpty();
    }
}
