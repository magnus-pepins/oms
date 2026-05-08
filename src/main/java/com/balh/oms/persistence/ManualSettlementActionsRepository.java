package com.balh.oms.persistence;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Four-eyes manual settlement instructions (Flyway {@code V11} {@code manual_settlement_actions}).
 */
@Repository
public class ManualSettlementActionsRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ManualSettlementActionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean executionExists(long executionId) {
        Integer n =
                jdbc.queryForObject(
                        "SELECT 1 FROM executions WHERE id = :id LIMIT 1",
                        new MapSqlParameterSource("id", executionId),
                        Integer.class);
        return n != null && n == 1;
    }

    public long insert(long executionId, String actionType, String requestedBy, String payloadJson) {
        var params =
                new MapSqlParameterSource()
                        .addValue("eid", executionId)
                        .addValue("atype", actionType)
                        .addValue("req", requestedBy)
                        .addValue("payload", payloadJson == null || payloadJson.isBlank() ? "{}" : payloadJson);
        Long id =
                jdbc.queryForObject(
                        """
                                INSERT INTO manual_settlement_actions (execution_id, action_type, requested_by, payload_json)
                                VALUES (:eid, :atype, :req, CAST(:payload AS JSONB))
                                RETURNING id
                                """,
                        params,
                        Long.class);
        return id == null ? 0L : id;
    }

    public Optional<ManualSettlementActionRow> findById(long id) {
        var params = new MapSqlParameterSource("id", id);
        List<ManualSettlementActionRow> rows =
                jdbc.query(
                        """
                                SELECT id, execution_id, action_type, requested_by,
                                       approved_by, COALESCE(payload_json::text, '{}') AS payload_json, created_at
                                FROM manual_settlement_actions WHERE id = :id
                                """,
                        params,
                        (rs, rn) -> mapRow(rs));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.getFirst());
    }

    /**
     * Lists newest first. Caller must pass {@code executionId} or both {@code from} and {@code to}
     * (half-open on {@code created_at}).
     */
    public List<ManualSettlementActionRow> findByFilters(
            Long executionId, Instant from, Instant to, int limit, int offset) {
        StringBuilder sql =
                new StringBuilder(
                        """
                                SELECT id, execution_id, action_type, requested_by,
                                       approved_by, COALESCE(payload_json::text, '{}') AS payload_json, created_at
                                FROM manual_settlement_actions
                                WHERE 1 = 1
                                """);
        var params = new MapSqlParameterSource();
        if (executionId != null) {
            sql.append(" AND execution_id = :eid");
            params.addValue("eid", executionId);
        }
        if (from != null) {
            sql.append(" AND created_at >= :from_ts");
            params.addValue("from_ts", Timestamp.from(from));
        }
        if (to != null) {
            sql.append(" AND created_at < :to_ts");
            params.addValue("to_ts", Timestamp.from(to));
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :lim OFFSET :off");
        params.addValue("lim", limit);
        params.addValue("off", offset);
        return jdbc.query(sql.toString(), params, (rs, rn) -> mapRow(rs));
    }

    /**
     * Sets {@code approved_by} when still null and approver is not the same identity as {@code
     * requested_by} (case-insensitive trim).
     */
    public ApproveManualSettlementResult approve(long id, String approvedBy) {
        String ab = approvedBy == null ? "" : approvedBy.trim();
        if (ab.isEmpty()) {
            return ApproveManualSettlementResult.INVALID_APPROVER;
        }
        Optional<ManualSettlementActionRow> row = findById(id);
        if (row.isEmpty()) {
            return ApproveManualSettlementResult.NOT_FOUND;
        }
        ManualSettlementActionRow r = row.get();
        if (r.approvedBy() != null && !r.approvedBy().isBlank()) {
            return ApproveManualSettlementResult.ALREADY_APPROVED;
        }
        if (equalsIgnoreCaseTrim(r.requestedBy(), ab)) {
            return ApproveManualSettlementResult.SAME_ACTOR;
        }
        int n =
                jdbc.update(
                        """
                                UPDATE manual_settlement_actions
                                SET approved_by = :ab
                                WHERE id = :id AND approved_by IS NULL
                                """,
                        new MapSqlParameterSource().addValue("ab", ab).addValue("id", id));
        if (n == 0) {
            return ApproveManualSettlementResult.CONFLICT;
        }
        return ApproveManualSettlementResult.OK;
    }

    private static boolean equalsIgnoreCaseTrim(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return a.trim().equalsIgnoreCase(b.trim());
    }

    private static ManualSettlementActionRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        String approved = rs.getString("approved_by");
        return new ManualSettlementActionRow(
                rs.getLong("id"),
                rs.getLong("execution_id"),
                rs.getString("action_type"),
                rs.getString("requested_by"),
                approved,
                rs.getString("payload_json"),
                rs.getTimestamp("created_at").toInstant());
    }
}
