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
            SELECT id, execution_id, to_settlement_status, leg_kind, payload_json::text AS payload_json,
                   attempts
            FROM ledger_settlement_outbox
            WHERE posted_at IS NULL
              AND skipped_at IS NULL
              AND created_at <= :older_than
            ORDER BY id
            LIMIT :lim
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_SKIPPED_SQL = """
            UPDATE ledger_settlement_outbox
            SET skipped_at = :ts, skip_reason = :reason
            WHERE id = :id AND posted_at IS NULL AND skipped_at IS NULL
            """;

    private static final String MARK_POSTED_SQL = """
            UPDATE ledger_settlement_outbox SET posted_at = :ts WHERE id = :id
            """;

    private static final String RECORD_ATTEMPT_SQL = """
            UPDATE ledger_settlement_outbox
            SET attempts = attempts + 1,
                last_error_text = :err,
                last_attempt_at = :ts
            WHERE id = :id
            """;

    private static final String FIND_STUCK_UNPOSTED_SQL = """
            SELECT id, execution_id, to_settlement_status, leg_kind,
                   payload_json::text AS payload_json, created_at, posted_at,
                   attempts, last_error_text, last_attempt_at
            FROM ledger_settlement_outbox
            WHERE posted_at IS NULL
              AND attempts >= :min_attempts
            ORDER BY last_attempt_at DESC NULLS LAST, id DESC
            LIMIT :lim
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
            long id,
            long executionId,
            String toSettlementStatus,
            String legKind,
            String payloadJson,
            int attempts) {}

    /** Timeline projection — includes timestamps so the UI can show the lifecycle. */
    public record OutboxTimelineRow(
            long id,
            long executionId,
            String toSettlementStatus,
            String legKind,
            String payloadJson,
            Instant createdAt,
            Instant postedAt) {}

    /** Ops read path — unposted rows with delivery attempt history (beard-admin stuck-outbox panel). */
    public record StuckOutboxRow(
            long id,
            long executionId,
            String toSettlementStatus,
            String legKind,
            String payloadJson,
            Instant createdAt,
            int attempts,
            String lastErrorTextOrNull,
            Instant lastAttemptAtOrNull) {}

    private static final RowMapper<OutboxRow> ROW_MAPPER =
            (rs, rowNum) ->
                    new OutboxRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getString("to_settlement_status"),
                            rs.getString("leg_kind"),
                            rs.getString("payload_json"),
                            rs.getInt("attempts"));

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

    /**
     * Records one reconciler delivery attempt. {@code errorText} is truncated to fit the column;
     * pass {@code null} on success (attempt still counted for stuck-row visibility).
     */
    /**
     * Tombstones an unpostable row so {@link com.balh.oms.reconciler.LedgerSettlementOutboxReconciler}
     * stops selecting it. Does not set {@code posted_at} (no Ledger delivery occurred).
     */
    public int markSkipped(long id, Instant skippedAt, String reason) {
        String r = reason;
        if (r != null && r.length() > 200) {
            r = r.substring(0, 200);
        }
        return jdbc.update(
                MARK_SKIPPED_SQL,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("ts", Timestamp.from(skippedAt))
                        .addValue("reason", r));
    }

    public void recordAttempt(long id, Instant attemptedAt, String errorText) {
        String err = errorText;
        if (err != null && err.length() > LAST_ERROR_TEXT_MAX_CHARS) {
            err = err.substring(0, LAST_ERROR_TEXT_MAX_CHARS);
        }
        jdbc.update(
                RECORD_ATTEMPT_SQL,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("ts", Timestamp.from(attemptedAt))
                        .addValue("err", err));
    }

    /** Unposted rows with at least {@code minAttempts} delivery tries, newest attempt first. */
    public List<StuckOutboxRow> findStuckUnposted(int minAttempts, int limit) {
        return jdbc.query(
                FIND_STUCK_UNPOSTED_SQL,
                new MapSqlParameterSource()
                        .addValue("min_attempts", Math.max(1, minAttempts))
                        .addValue("lim", Math.max(1, limit)),
                (rs, rowNum) -> {
                    Timestamp lastAttempt = rs.getTimestamp("last_attempt_at");
                    return new StuckOutboxRow(
                            rs.getLong("id"),
                            rs.getLong("execution_id"),
                            rs.getString("to_settlement_status"),
                            rs.getString("leg_kind"),
                            rs.getString("payload_json"),
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getInt("attempts"),
                            rs.getString("last_error_text"),
                            lastAttempt == null ? null : lastAttempt.toInstant());
                });
    }

    /** Truncation bound for {@code last_error_text} (matches Flyway column intent). */
    public static final int LAST_ERROR_TEXT_MAX_CHARS = 2000;

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
