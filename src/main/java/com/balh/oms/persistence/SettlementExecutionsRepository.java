package com.balh.oms.persistence;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Read-only listing of executions joined to orders for internal settlement / ops UIs.
 */
@Repository
public class SettlementExecutionsRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementExecutionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String DETAIL_SELECT =
            """
                    SELECT e.id,
                           e.order_id,
                           e.account_id,
                           e.venue_id,
                           e.venue_ts,
                           e.venue_exec_ref,
                           e.last_quantity,
                           e.last_price,
                           e.leaves_quantity,
                           e.cum_quantity_after,
                           e.exec_type::text AS exec_type,
                           e.settlement_status::text AS settlement_status,
                           e.created_at,
                           o.status::text AS order_status,
                           o.side::text AS side,
                           o.instrument_symbol,
                           e.trade_date,
                           e.expected_settlement_date,
                           COALESCE(e.raw_envelope_json::text, '{}') AS raw_envelope_json
                    FROM executions e
                    INNER JOIN orders o ON o.id = e.order_id
                    WHERE e.id = :id
                    """;

    private static final String FIND_NON_TERMINAL_TRADE_IDS_SQL =
            """
                    SELECT id FROM executions
                    WHERE exec_type = 'TRADE'
                      AND settlement_status NOT IN ('settled', 'failed')
                      AND settlement_auto_step_failures < :max_advance_failures
                      AND created_at >= NOW() - make_interval(secs => :max_age_seconds)
                    ORDER BY created_at
                    LIMIT :lim
                    """;

    /**
     * Returns ids of recent TRADE executions that have not yet reached a terminal settlement
     * status. Used by {@link com.balh.oms.settlement.SettlementAutoStepScheduler} to walk fresh
     * fills through the §12.3 state machine on a configurable cadence so beard-admin can show
     * the transitions live. The {@code maxAgeSeconds} bound stops the scheduler from picking up
     * stale rows from prior runs / replays.
     */
    public List<Long> findNonTerminalTradeIds(long maxAgeSeconds, int maxAdvanceFailures, int limit) {
        var params = new MapSqlParameterSource()
                .addValue("max_age_seconds", maxAgeSeconds)
                .addValue("max_advance_failures", Math.max(1, maxAdvanceFailures))
                .addValue("lim", limit);
        return jdbc.query(FIND_NON_TERMINAL_TRADE_IDS_SQL, params, (rs, rowNum) -> rs.getLong("id"));
    }

    /** Single execution joined to its order, or empty if missing or orphaned (no order row). */
    public Optional<SettlementExecutionDetailRow> findById(long executionId) {
        var params = new MapSqlParameterSource("id", executionId);
        List<SettlementExecutionDetailRow> list =
                jdbc.query(
                        DETAIL_SELECT,
                        params,
                        (rs, rowNum) -> {
                            Date tradeDate = rs.getDate("trade_date");
                            Date expectedSettlementDate = rs.getDate("expected_settlement_date");
                            return new SettlementExecutionDetailRow(
                                    rs.getLong("id"),
                                    rs.getObject("order_id", UUID.class),
                                    rs.getObject("account_id", UUID.class),
                                    rs.getString("venue_id"),
                                    rs.getTimestamp("venue_ts").toInstant(),
                                    rs.getString("venue_exec_ref"),
                                    rs.getBigDecimal("last_quantity"),
                                    rs.getBigDecimal("last_price"),
                                    rs.getBigDecimal("leaves_quantity"),
                                    rs.getBigDecimal("cum_quantity_after"),
                                    rs.getString("exec_type"),
                                    rs.getString("settlement_status"),
                                    rs.getTimestamp("created_at").toInstant(),
                                    rs.getString("order_status"),
                                    rs.getString("side"),
                                    rs.getString("instrument_symbol"),
                                    tradeDate == null ? null : tradeDate.toLocalDate(),
                                    expectedSettlementDate == null ? null : expectedSettlementDate.toLocalDate(),
                                    rs.getString("raw_envelope_json"));
                        });
        if (list.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(list.getFirst());
    }

    /**
     * Newest-first. Caller must constrain by {@code orderId} and/or {@code from}/{@code to} on
     * {@code executions.created_at}.
     */
    public List<SettlementExecutionRow> findByFilters(
            UUID orderId, Instant from, Instant to, String settlementStatus, int limit, int offset) {
        StringBuilder sql = new StringBuilder(
                """
                        SELECT e.id,
                               e.order_id,
                               e.account_id,
                               e.venue_id,
                               e.venue_ts,
                               e.venue_exec_ref,
                               e.last_quantity,
                               e.last_price,
                               e.leaves_quantity,
                               e.cum_quantity_after,
                               e.exec_type::text AS exec_type,
                               e.settlement_status::text AS settlement_status,
                               e.created_at,
                               o.status::text AS order_status,
                               o.side::text AS side,
                               o.instrument_symbol,
                               e.trade_date,
                               e.expected_settlement_date
                        FROM executions e
                        INNER JOIN orders o ON o.id = e.order_id
                        WHERE 1 = 1
                        """);
        var params = new MapSqlParameterSource();
        if (orderId != null) {
            sql.append(" AND e.order_id = :order_id");
            params.addValue("order_id", orderId);
        }
        if (from != null) {
            sql.append(" AND e.created_at >= :from_ts");
            params.addValue("from_ts", Timestamp.from(from));
        }
        if (to != null) {
            sql.append(" AND e.created_at < :to_ts");
            params.addValue("to_ts", Timestamp.from(to));
        }
        if (settlementStatus != null) {
            sql.append(" AND e.settlement_status = CAST(:settlement_status AS execution_settlement_status)");
            params.addValue("settlement_status", settlementStatus);
        }
        sql.append(" ORDER BY e.created_at DESC, e.id DESC LIMIT :lim OFFSET :off");
        params.addValue("lim", limit);
        params.addValue("off", offset);
        return jdbc.query(
                sql.toString(),
                params,
                (rs, rowNum) -> {
                    Date tradeDate = rs.getDate("trade_date");
                    Date expectedSettlementDate = rs.getDate("expected_settlement_date");
                    return new SettlementExecutionRow(
                            rs.getLong("id"),
                            rs.getObject("order_id", UUID.class),
                            rs.getObject("account_id", UUID.class),
                            rs.getString("venue_id"),
                            rs.getTimestamp("venue_ts").toInstant(),
                            rs.getString("venue_exec_ref"),
                            rs.getBigDecimal("last_quantity"),
                            rs.getBigDecimal("last_price"),
                            rs.getBigDecimal("leaves_quantity"),
                            rs.getBigDecimal("cum_quantity_after"),
                            rs.getString("exec_type"),
                            rs.getString("settlement_status"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getString("order_status"),
                            rs.getString("side"),
                            rs.getString("instrument_symbol"),
                            tradeDate == null ? null : tradeDate.toLocalDate(),
                            expectedSettlementDate == null ? null : expectedSettlementDate.toLocalDate());
                });
    }
}
