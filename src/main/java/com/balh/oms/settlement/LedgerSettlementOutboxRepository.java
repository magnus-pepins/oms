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

    /** Default leg kind, used by older call sites that pre-date V39 multi-leg. */
    public static final String LEG_CASH = "cash";

    /** Cross-currency cash legs (Phase 2: two single-currency legs via @FX-Suspense). */
    public static final String LEG_CASH_BASE = "cash-base";

    public static final String LEG_CASH_QUOTE = "cash-quote";

    /** Commission revenue leg. */
    public static final String LEG_FEE = "fee";

    private static final String INSERT_IGNORE = """
            INSERT INTO ledger_settlement_outbox (execution_id, to_settlement_status, leg_kind, payload_json)
            VALUES (:eid, :st, :leg, CAST(:payload AS JSONB))
            ON CONFLICT (execution_id, to_settlement_status, leg_kind) DO NOTHING
            """;

    private static final String LOCK_UNPOSTED_SQL = """
            SELECT id, execution_id, to_settlement_status, leg_kind, payload_json::text AS payload_json
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

    // Timeline read path: returns every leg for one execution with both timestamps so
    // the beard-admin Detail panel can render a settlement lifecycle. created_at is
    // the moment the leg was enqueued (effectively when OMS reached settled CAS);
    // posted_at is when the reconciler successfully posted it to Ledger (or NULL on
    // pending / failed). Ordered by (leg_kind, id) so cash always comes before fee.
    private static final String FIND_BY_EXECUTION_SQL = """
            SELECT id, execution_id, to_settlement_status, leg_kind,
                   payload_json::text AS payload_json, created_at, posted_at
            FROM ledger_settlement_outbox
            WHERE execution_id = :eid
            ORDER BY leg_kind, id
            """;

    public record OutboxRow(
            long id, long executionId, String toSettlementStatus, String legKind, String payloadJson) {}

    /** Timeline projection — includes timestamps so the UI can show the lifecycle. */
    public record OutboxTimelineRow(
            long id,
            long executionId,
            String toSettlementStatus,
            String legKind,
            String payloadJson,
            Instant createdAt,
            Instant postedAt) {}

    private static final RowMapper<OutboxRow> ROW_MAPPER =
            (rs, rowNum) ->
                    new OutboxRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getString("to_settlement_status"),
                            rs.getString("leg_kind"),
                            rs.getString("payload_json"));

    private final NamedParameterJdbcTemplate jdbc;

    public LedgerSettlementOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Pre-V39 callers default to the {@code cash} leg. @return number of rows inserted (0 if duplicate) */
    public int insertIgnore(long executionId, String toSettlementStatus, String payloadJson) {
        return insertIgnore(executionId, toSettlementStatus, LEG_CASH, payloadJson);
    }

    /** @return number of rows inserted (0 if duplicate on (execution_id, to_settlement_status, leg_kind)) */
    public int insertIgnore(long executionId, String toSettlementStatus, String legKind, String payloadJson) {
        if (legKind == null || legKind.isBlank()) {
            throw new IllegalArgumentException("legKind required");
        }
        return jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("eid", executionId)
                        .addValue("st", toSettlementStatus)
                        .addValue("leg", legKind)
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

    /** Used by the settlement timeline read path (SettlementController#getTimeline). */
    public List<OutboxTimelineRow> findByExecution(long executionId) {
        return jdbc.query(
                FIND_BY_EXECUTION_SQL,
                new MapSqlParameterSource("eid", executionId),
                (rs, rowNum) -> {
                    Timestamp posted = rs.getTimestamp("posted_at");
                    return new OutboxTimelineRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getString("to_settlement_status"),
                            rs.getString("leg_kind"),
                            rs.getString("payload_json"),
                            rs.getTimestamp("created_at").toInstant(),
                            posted == null ? null : posted.toInstant());
                });
    }
}
