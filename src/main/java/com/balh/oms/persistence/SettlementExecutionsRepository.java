package com.balh.oms.persistence;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
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
                               o.instrument_symbol
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
                (rs, rowNum) ->
                        new SettlementExecutionRow(
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
                                rs.getString("instrument_symbol")));
    }
}
