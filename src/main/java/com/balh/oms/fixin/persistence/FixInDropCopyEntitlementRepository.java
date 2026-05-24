package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class FixInDropCopyEntitlementRepository {

    private static final String SELECT_SESSIONS = """
            SELECT drop_copy_session_id
              FROM oms_fix_drop_copy_entitlement
             WHERE oms_account_id = :oms_account_id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInDropCopyEntitlementRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<UUID> findDropCopySessionIdsForAccount(UUID omsAccountId) {
        return jdbc.queryForList(
                SELECT_SESSIONS, new MapSqlParameterSource("oms_account_id", omsAccountId), UUID.class);
    }
}
