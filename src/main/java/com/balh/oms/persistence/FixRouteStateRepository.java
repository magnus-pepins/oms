package com.balh.oms.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Postgres gate for FIX outbound send ({@code fix_route_state.send_enabled}).
 */
@Repository
public class FixRouteStateRepository {

    private static final String SELECT_SQL = """
            SELECT route_key, send_enabled, updated_at, updated_by, note
            FROM fix_route_state WHERE route_key = :route_key
            """;

    private static final String UPDATE_SQL = """
            UPDATE fix_route_state
               SET send_enabled = :send_enabled,
                   updated_at = NOW(),
                   updated_by = :updated_by,
                   note = :note
             WHERE route_key = :route_key
            """;

    private static final RowMapper<FixRouteStateRow> ROW_MAPPER = (rs, rowNum) -> new FixRouteStateRow(
            rs.getString("route_key"),
            rs.getBoolean("send_enabled"),
            rs.getTimestamp("updated_at").toInstant(),
            rs.getString("updated_by"),
            rs.getString("note"));

    private final NamedParameterJdbcTemplate jdbc;

    public FixRouteStateRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<FixRouteStateRow> findByRouteKey(String routeKey) {
        var params = new MapSqlParameterSource("route_key", routeKey);
        var rows = jdbc.query(SELECT_SQL, params, ROW_MAPPER);
        return rows.stream().findFirst();
    }

    /**
     * @return {@code true} if the row existed and was updated
     */
    public boolean updateSendEnabled(String routeKey, boolean sendEnabled, String updatedBy, String note) {
        var params = new MapSqlParameterSource()
                .addValue("route_key", routeKey)
                .addValue("send_enabled", sendEnabled)
                .addValue("updated_by", updatedBy)
                .addValue("note", note);
        return jdbc.update(UPDATE_SQL, params) == 1;
    }

    /**
     * Start-of-day style reconciliation: enable outbound send on every route row (operator feature-flagged).
     *
     * @return number of rows updated
     */
    public int sodEnableSendOnAllRoutes(String updatedBy) {
        String sql = """
                UPDATE fix_route_state
                   SET send_enabled = TRUE,
                       updated_at = NOW(),
                       updated_by = :updated_by
                """;
        return jdbc.update(sql, new MapSqlParameterSource("updated_by", updatedBy));
    }
}
