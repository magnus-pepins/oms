package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class BrokerSettlementConfirmRepository {

    private static final String INSERT_IGNORE_SQL = """
            INSERT INTO broker_settlement_confirm (execution_id) VALUES (:eid)
            ON CONFLICT (execution_id) DO NOTHING
            """;

    private static final String SELECT_PENDING_FOR_UPDATE = """
            SELECT id, execution_id FROM broker_settlement_confirm
            WHERE applied_at IS NULL
            ORDER BY id
            LIMIT :lim
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_APPLIED_SQL = """
            UPDATE broker_settlement_confirm SET applied_at = :ts WHERE id = :id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerSettlementConfirmRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertIgnore(long executionId) {
        return jdbc.update(
                INSERT_IGNORE_SQL, new MapSqlParameterSource("eid", executionId));
    }

    public List<PendingConfirmRow> lockPendingBatch(int limit) {
        return jdbc.query(
                SELECT_PENDING_FOR_UPDATE,
                new MapSqlParameterSource("lim", limit),
                (rs, rowNum) ->
                        new PendingConfirmRow(rs.getLong("id"), rs.getLong("execution_id")));
    }

    private static final String DELETE_PENDING_FOR_EXECUTION = """
            DELETE FROM broker_settlement_confirm
            WHERE execution_id = :eid AND applied_at IS NULL
            """;

    public int deletePendingForExecution(long executionId) {
        return jdbc.update(
                DELETE_PENDING_FOR_EXECUTION, new MapSqlParameterSource("eid", executionId));
    }

    public void markApplied(long confirmRowId, Instant at) {
        jdbc.update(
                MARK_APPLIED_SQL,
                new MapSqlParameterSource()
                        .addValue("id", confirmRowId)
                        .addValue("ts", Timestamp.from(at)));
    }

    /**
     * Read the (at most one) broker confirm row for an execution, including its
     * created_at (when broker delivered confirmation → maps to OMS 'matched' phase)
     * and applied_at (when SettlementConfirmProcessor consumed it → 'confirmed' phase).
     * Used by the settlement timeline read path (SettlementController#getTimeline).
     */
    public java.util.Optional<ConfirmTimelineRow> findByExecution(long executionId) {
        return jdbc.query(
                "SELECT id, execution_id, created_at, applied_at "
                        + "FROM broker_settlement_confirm WHERE execution_id = :eid",
                new MapSqlParameterSource("eid", executionId),
                (rs, rowNum) -> {
                    Timestamp applied = rs.getTimestamp("applied_at");
                    return new ConfirmTimelineRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getTimestamp("created_at").toInstant(),
                            applied == null ? null : applied.toInstant());
                })
                .stream()
                .findFirst();
    }

    public record PendingConfirmRow(long id, long executionId) {}

    public record ConfirmTimelineRow(
            long id, long executionId, Instant createdAt, Instant appliedAt) {}
}
