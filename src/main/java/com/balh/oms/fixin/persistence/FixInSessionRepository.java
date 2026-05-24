package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class FixInSessionRepository {

    private static final String SELECT_ENABLED = """
            SELECT id, counterparty_id, environment, session_mode,
                   sender_comp_id, target_comp_id, session_qualifier,
                   logon_username, password_hash, heartbeat_seconds, enabled
              FROM oms_fix_in_session
             WHERE enabled = TRUE
            """;

    private static final String SELECT_BY_COMP_IDS = """
            SELECT id, counterparty_id, environment, session_mode,
                   sender_comp_id, target_comp_id, session_qualifier,
                   logon_username, password_hash, heartbeat_seconds, enabled
              FROM oms_fix_in_session
             WHERE sender_comp_id = :sender_comp_id
               AND target_comp_id = :target_comp_id
               AND COALESCE(session_qualifier, '') = COALESCE(:session_qualifier, '')
            """;

    private static final RowMapper<FixInSessionRow> ROW_MAPPER = (rs, rowNum) -> new FixInSessionRow(
            rs.getObject("id", UUID.class),
            rs.getObject("counterparty_id", UUID.class),
            rs.getString("environment"),
            rs.getString("session_mode"),
            rs.getString("sender_comp_id"),
            rs.getString("target_comp_id"),
            rs.getString("session_qualifier"),
            rs.getString("logon_username"),
            rs.getString("password_hash"),
            rs.getInt("heartbeat_seconds"),
            rs.getBoolean("enabled"));

    private final NamedParameterJdbcTemplate jdbc;

    public FixInSessionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<FixInSessionRow> findEnabled() {
        return jdbc.query(SELECT_ENABLED, ROW_MAPPER);
    }

    public Optional<FixInSessionRow> findByWireCompIds(
            String clientSenderCompId, String omsTargetCompId, String sessionQualifierOrNull) {
        var params = new MapSqlParameterSource()
                .addValue("sender_comp_id", clientSenderCompId)
                .addValue("target_comp_id", omsTargetCompId)
                .addValue("session_qualifier", sessionQualifierOrNull);
        return jdbc.query(SELECT_BY_COMP_IDS, params, ROW_MAPPER).stream().findFirst();
    }

    public Optional<FixInSessionRow> findById(UUID id) {
        String sql = """
                SELECT id, counterparty_id, environment, session_mode,
                       sender_comp_id, target_comp_id, session_qualifier,
                       logon_username, password_hash, heartbeat_seconds, enabled
                  FROM oms_fix_in_session WHERE id = :id
                """;
        return jdbc.query(sql, new MapSqlParameterSource("id", id), ROW_MAPPER).stream().findFirst();
    }

    public List<FixInSessionRow> findAll() {
        String sql = """
                SELECT id, counterparty_id, environment, session_mode,
                       sender_comp_id, target_comp_id, session_qualifier,
                       logon_username, password_hash, heartbeat_seconds, enabled
                  FROM oms_fix_in_session
                 ORDER BY sender_comp_id, target_comp_id
                """;
        return jdbc.query(sql, ROW_MAPPER);
    }

    public boolean updateEnabled(UUID id, boolean enabled) {
        return jdbc.update(
                        "UPDATE oms_fix_in_session SET enabled = :enabled, updated_at = NOW() WHERE id = :id",
                        new MapSqlParameterSource().addValue("id", id).addValue("enabled", enabled))
                == 1;
    }

    public boolean updatePasswordHash(UUID id, String passwordHash) {
        return jdbc.update(
                        """
                        UPDATE oms_fix_in_session
                           SET password_hash = :password_hash, updated_at = NOW()
                         WHERE id = :id
                        """,
                        new MapSqlParameterSource()
                                .addValue("id", id)
                                .addValue("password_hash", passwordHash))
                == 1;
    }
}
