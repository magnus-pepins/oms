package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class FixInSessionAdminActionRepository {

    private static final String INSERT = """
            INSERT INTO oms_fix_session_admin_actions (
                id, session_role, fix_session_id, broker_route_key, action_type,
                requested_by, approved_by, reason, counterparty_reference, payload_json)
            VALUES (
                :id, :session_role, :fix_session_id, :broker_route_key, :action_type,
                :requested_by, :approved_by, :reason, :counterparty_reference, :payload_json)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInSessionAdminActionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String LIST = """
            SELECT id, session_role, fix_session_id, broker_route_key, action_type,
                   requested_by, approved_by, reason, counterparty_reference, payload_json, created_at
              FROM oms_fix_session_admin_actions
             WHERE (:fix_session_id IS NULL OR fix_session_id = :fix_session_id)
             ORDER BY created_at DESC
             LIMIT :limit
            """;

    public List<FixInSessionAdminActionRow> list(UUID fixSessionIdOrNull, int limit) {
        return jdbc.query(
                LIST,
                new MapSqlParameterSource()
                        .addValue("fix_session_id", fixSessionIdOrNull)
                        .addValue("limit", Math.min(200, Math.max(1, limit))),
                (rs, rowNum) -> new FixInSessionAdminActionRow(
                        rs.getObject("id", UUID.class),
                        rs.getString("session_role"),
                        rs.getObject("fix_session_id", UUID.class),
                        rs.getString("broker_route_key"),
                        rs.getString("action_type"),
                        rs.getString("requested_by"),
                        rs.getString("approved_by"),
                        rs.getString("reason"),
                        rs.getString("counterparty_reference"),
                        rs.getString("payload_json"),
                        rs.getTimestamp("created_at").toInstant()));
    }

    public void insert(FixInSessionAdminActionRow row) {
        jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("id", row.id())
                        .addValue("session_role", row.sessionRole())
                        .addValue("fix_session_id", row.fixSessionIdOrNull())
                        .addValue("broker_route_key", row.brokerRouteKeyOrNull())
                        .addValue("action_type", row.actionType())
                        .addValue("requested_by", row.requestedBy())
                        .addValue("approved_by", row.approvedByOrNull())
                        .addValue("reason", row.reason())
                        .addValue("counterparty_reference", row.counterpartyReferenceOrNull())
                        .addValue("payload_json", row.payloadJsonOrNull()));
    }
}
