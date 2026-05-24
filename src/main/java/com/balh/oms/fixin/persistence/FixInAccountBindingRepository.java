package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public class FixInAccountBindingRepository {

    private static final String SELECT_BY_SESSION_AND_TAG = """
            SELECT id, session_id, fix_account_tag, oms_account_id,
                   ledger_identity_id, ledger_balance_id, default_binding, enabled
              FROM oms_fix_in_account_binding
             WHERE session_id = :session_id
               AND fix_account_tag = :fix_account_tag
               AND enabled = TRUE
            """;

    private static final String SELECT_DEFAULT = """
            SELECT id, session_id, fix_account_tag, oms_account_id,
                   ledger_identity_id, ledger_balance_id, default_binding, enabled
              FROM oms_fix_in_account_binding
             WHERE session_id = :session_id
               AND default_binding = TRUE
               AND enabled = TRUE
             ORDER BY created_at
             LIMIT 1
            """;

    private static final RowMapper<FixInAccountBindingRow> ROW_MAPPER = (rs, rowNum) -> new FixInAccountBindingRow(
            rs.getObject("id", UUID.class),
            rs.getObject("session_id", UUID.class),
            rs.getString("fix_account_tag"),
            rs.getObject("oms_account_id", UUID.class),
            rs.getString("ledger_identity_id"),
            rs.getString("ledger_balance_id"),
            rs.getBoolean("default_binding"),
            rs.getBoolean("enabled"));

    private final NamedParameterJdbcTemplate jdbc;

    public FixInAccountBindingRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<FixInAccountBindingRow> findBySessionAndAccountTag(UUID sessionId, String fixAccountTagOrEmpty) {
        var params = new MapSqlParameterSource()
                .addValue("session_id", sessionId)
                .addValue("fix_account_tag", fixAccountTagOrEmpty == null ? "" : fixAccountTagOrEmpty);
        return jdbc.query(SELECT_BY_SESSION_AND_TAG, params, ROW_MAPPER).stream().findFirst();
    }

    public Optional<FixInAccountBindingRow> findDefaultForSession(UUID sessionId) {
        return jdbc.query(
                        SELECT_DEFAULT,
                        new MapSqlParameterSource("session_id", sessionId),
                        ROW_MAPPER)
                .stream()
                .findFirst();
    }
}
