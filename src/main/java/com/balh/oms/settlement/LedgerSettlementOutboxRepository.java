package com.balh.oms.settlement;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Idempotent enqueue of Ledger settlement postings (same transaction as OMS {@code settled} CAS when enabled).
 */
@Repository
public class LedgerSettlementOutboxRepository {

    private static final String INSERT_IGNORE = """
            INSERT INTO ledger_settlement_outbox (execution_id, to_settlement_status, payload_json)
            VALUES (:eid, :st, CAST(:payload AS JSONB))
            ON CONFLICT (execution_id, to_settlement_status) DO NOTHING
            """;

    private static final String LOCK_UNPOSTED_SQL = """
            SELECT id, execution_id, to_settlement_status, payload_json::text AS payload_json
            FROM ledger_settlement_outbox
            WHERE posted_at IS NULL
              AND created_at <= :older_than
            ORDER BY id
            LIMIT :lim
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_POSTED_SQL = """
            UPDATE ledger_settlement_outbox SET posted_at = :ts WHERE id = :id
            """;

    public record OutboxRow(long id, long executionId, String toSettlementStatus, String payloadJson) {}

    private static final RowMapper<OutboxRow> ROW_MAPPER =
            (rs, rowNum) ->
                    new OutboxRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getString("to_settlement_status"),
                            rs.getString("payload_json"));

    private final NamedParameterJdbcTemplate jdbc;

    public LedgerSettlementOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return number of rows inserted (0 if duplicate) */
    public int insertIgnore(long executionId, String toSettlementStatus, String payloadJson) {
        return jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("eid", executionId)
                        .addValue("st", toSettlementStatus)
                        .addValue("payload", payloadJson == null || payloadJson.isBlank() ? "{}" : payloadJson));
    }

    /**
     * Locks up to {@code limit} unposted rows for delivery (caller must run inside a transaction).
     */
    public List<OutboxRow> lockUnpostedOlderThan(Instant olderThan, int limit) {
        return jdbc.query(
                LOCK_UNPOSTED_SQL,
                new MapSqlParameterSource()
                        .addValue("older_than", Timestamp.from(olderThan))
                        .addValue("lim", limit),
                ROW_MAPPER);
    }

    public void markPosted(long id, Instant postedAt) {
        jdbc.update(
                MARK_POSTED_SQL,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("ts", Timestamp.from(postedAt)));
    }
}
