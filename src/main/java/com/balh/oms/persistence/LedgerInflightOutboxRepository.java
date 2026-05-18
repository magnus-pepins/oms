package com.balh.oms.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Postgres transactional outbox for Ledger BUY inflight holds when async mode is on.
 */
@Repository
public class LedgerInflightOutboxRepository {

    private static final int MAX_STORED_ERROR_CHARS = 4000;

    private final NamedParameterJdbcTemplate jdbc;

    public LedgerInflightOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record InflightRow(long id, UUID orderId, String payloadJson, Instant createdAt, int attempts) {}

    /**
     * Slice 4p row shape for {@link com.balh.oms.reconciler.LedgerInflightHoldFailureCompensator}:
     * carries enough metadata to log + emit a {@code CancelOrderCommand}, but not the full
     * payload (the compensator does not call Ledger).
     *
     * <p>Phase 4 Tier 2.5 phase E-2 added {@link #shardId}: the order's
     * {@code orders.shard_id}, sourced via a JOIN in {@link #FETCH_FAILED_UNCOMPENSATED_SQL}
     * (the existing {@code FK ledger_inflight_outbox.order_id REFERENCES orders(id)} guarantees
     * the join is total). The compensator passes {@code shardId} to
     * {@code OmsClusterShardRouter.forShard(int)} so the resulting
     * {@code CancelOrderCommand} lands on the cluster that owns the order. At
     * {@code shardCount=1} every row carries {@code shardId=0} and the router resolves to the
     * single client — byte-identical to the pre-E-2 path.
     */
    public record FailedInflightRow(long id, UUID orderId, int attempts, String lastError, int shardId) {}

    private static final String INSERT_SQL = """
            INSERT INTO ledger_inflight_outbox (order_id, payload_json, created_at)
            VALUES (:order_id, CAST(:payload AS jsonb), :created_at)
            """;

    /**
     * Phase 4 Tier 2.5 phase D-1: idempotent INSERT used by the projector to backfill the row
     * if the ingress JVM crashed between cluster admit and its outbox-INSERT transaction commit.
     *
     * <p>Relies on the existing {@code uq_ledger_inflight_outbox_order_id} unique index from
     * V4. {@code ON CONFLICT (order_id) DO NOTHING} makes happy-path (ingress already wrote the
     * row) a no-op, and the crash-window path produces the same row the ingress would have
     * produced — so the slice 4p reconciler/compensator pipeline picks it up without changes.
     */
    private static final String INSERT_IF_ABSENT_SQL = """
            INSERT INTO ledger_inflight_outbox (order_id, payload_json, created_at)
            VALUES (:order_id, CAST(:payload AS jsonb), :created_at)
            ON CONFLICT (order_id) DO NOTHING
            """;

    /**
     * {@code FOR UPDATE SKIP LOCKED} — callers must run inside a Spring transaction that spans fetch +
     * {@link #markPublished}/{@link #markFailed} (see {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler}).
     *
     * <p>Slice 4p — adds {@code AND compensated_at IS NULL}: a row picked up by
     * {@link com.balh.oms.reconciler.LedgerInflightHoldFailureCompensator} for
     * {@link #markCompensated} is no longer eligible for reconciler retries (the order has been
     * cancelled in the cluster, so re-driving the Ledger hold would be incorrect).
     */
    private static final String FETCH_PENDING_SQL = """
            SELECT id, order_id, payload_json::text AS payload_json, created_at, attempts
            FROM ledger_inflight_outbox
            WHERE published_at IS NULL
              AND compensated_at IS NULL
              AND created_at <= :older_than
            ORDER BY id
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """;

    /**
     * Slice 4p — failed rows that the compensator should cancel. {@code attempts >= :threshold}
     * keeps transient failures (one-off network blip during a single tick) on the reconciler
     * retry path; a row that has crossed the threshold is treated as a hold the Ledger will not
     * accept (e.g. insufficient balance) and graduates to compensation.
     *
     * <p>Phase 4 Tier 2.5 phase E-2: JOINs {@code orders} so the compensator can route the
     * resulting {@link com.balh.oms.cluster.CancelOrderCommand} to the cluster that owns the
     * order's shard. The FK {@code ledger_inflight_outbox.order_id REFERENCES orders(id)}
     * (slice 4p {@code V4} migration) plus the projector's D-9 invariant
     * (orders row inserted before the inflight outbox row, both inside one projector tx)
     * make the join total. {@code FOR UPDATE SKIP LOCKED} stays on
     * {@code ledger_inflight_outbox} only — taking the row lock here is sufficient because
     * compensator concurrency is partitioned by the inflight outbox row, not by the order.
     */
    private static final String FETCH_FAILED_UNCOMPENSATED_SQL = """
            SELECT lio.id, lio.order_id, lio.attempts, lio.last_error, o.shard_id
            FROM ledger_inflight_outbox lio
            JOIN orders o ON o.id = lio.order_id
            WHERE lio.published_at IS NULL
              AND lio.compensated_at IS NULL
              AND lio.attempts >= :threshold
            ORDER BY lio.id
            LIMIT :batch_size
            FOR UPDATE OF lio SKIP LOCKED
            """;

    private static final String MARK_PUBLISHED_SQL = """
            UPDATE ledger_inflight_outbox
               SET published_at = :published_at
             WHERE id = :id
            """;

    private static final String MARK_FAILED_SQL = """
            UPDATE ledger_inflight_outbox
               SET attempts = attempts + 1,
                   last_attempt_at = :now,
                   last_error = :error
             WHERE id = :id
            """;

    private static final String MARK_COMPENSATED_SQL = """
            UPDATE ledger_inflight_outbox
               SET compensated_at = :compensated_at,
                   cancel_correlation_id = :cancel_correlation_id
             WHERE id = :id
            """;

    // ---- Wed-demo: inflight lifecycle reconciler (V32) -------------------------------------

    /**
     * Atomic upsert of the Ledger-returned {@code transactionId} after a successful
     * {@code POST /transactions} on the published row. Idempotent: if the row already has a
     * non-null {@code ledger_txn_id}, the UPDATE is a no-op rather than overwriting (the first
     * Ledger txn id wins; a retry of the publish on the async path would resurface the same id
     * via Ledger's {@code reference}-based dedupe).
     */
    private static final String SET_LEDGER_TXN_ID_SQL = """
            UPDATE ledger_inflight_outbox
               SET ledger_txn_id = :ledger_txn_id
             WHERE id = :id
               AND ledger_txn_id IS NULL
            """;

    /**
     * Settle-eligible working set for the {@code LedgerInflightLifecycleReconciler}: rows where
     * the hold has been published to Ledger ({@code ledger_txn_id IS NOT NULL}), the lifecycle
     * has not been settled, the row has not been compensated, the lifecycle attempt budget has
     * not been exhausted, the matching {@code orders} row is terminal, AND the per-row backoff
     * has elapsed since the last attempt (or no attempt yet).
     *
     * <p>JOIN to {@code orders}: the FK from V4 guarantees totality. We read
     * {@code orders.status} and {@code orders.shard_id} so the reconciler knows whether to
     * issue {@code commit} (FILLED) or {@code void} (CANCELLED / REJECTED / EXPIRED) and which
     * Ledger shim it would route through (single-cluster today; cross-shard later). FOR UPDATE
     * OF lio SKIP LOCKED so multiple reconciler JVMs don't double-settle the same hold.
     */
    private static final String FETCH_LIFECYCLE_SETTLEABLE_SQL = """
            SELECT lio.id,
                   lio.order_id,
                   lio.ledger_txn_id,
                   lio.lifecycle_attempts,
                   o.status        AS order_status,
                   o.shard_id      AS shard_id
              FROM ledger_inflight_outbox lio
              JOIN orders o ON o.id = lio.order_id
             WHERE lio.published_at IS NOT NULL
               AND lio.ledger_txn_id IS NOT NULL
               AND lio.lifecycle_settled_at IS NULL
               AND lio.compensated_at IS NULL
               AND lio.lifecycle_attempts < :attempts_threshold
               AND (lio.lifecycle_last_attempt_at IS NULL
                    OR lio.lifecycle_last_attempt_at <= :backoff_floor)
               AND o.status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')
             ORDER BY lio.id
             LIMIT :batch_size
             FOR UPDATE OF lio SKIP LOCKED
            """;

    private static final String MARK_LIFECYCLE_SETTLED_SQL = """
            UPDATE ledger_inflight_outbox
               SET lifecycle_settled_at      = :settled_at,
                   lifecycle_settled_action  = :action,
                   lifecycle_last_attempt_at = :settled_at
             WHERE id = :id
            """;

    private static final String MARK_LIFECYCLE_FAILED_SQL = """
            UPDATE ledger_inflight_outbox
               SET lifecycle_attempts        = lifecycle_attempts + 1,
                   lifecycle_last_attempt_at = :now,
                   lifecycle_last_error      = :error
             WHERE id = :id
            """;

    /**
     * Wed-demo (V32) row shape consumed by {@code LedgerInflightLifecycleReconciler}.
     * Carries everything the reconciler needs to compose a {@code PUT /transactions/inflight/{txID}}
     * call (txn id, intended action derived from {@code orderStatus}) plus the {@code shardId}
     * for the future cross-cluster routing (E-2-style).
     */
    public record LifecycleSettleableRow(
            long id,
            UUID orderId,
            String ledgerTxnId,
            int lifecycleAttempts,
            String orderStatus,
            int shardId) {}

    public void insert(UUID orderId, String payloadJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("payload", payloadJson)
                .addValue("created_at", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
    }

    /**
     * Phase 4 Tier 2.5 phase D-1: idempotent variant of {@link #insert(UUID, String)} used by
     * {@link com.balh.oms.projector.OmsPostgresProjector} to backfill the row if the ingress
     * JVM crashed between cluster admit and outbox-INSERT commit. See {@link #INSERT_IF_ABSENT_SQL}.
     *
     * @return {@code true} when this call inserted the row (crash-backfill path), {@code false}
     *     when the row already existed (happy path — ingress wrote it before crashing or
     *     committed cleanly).
     */
    public boolean insertIfAbsent(UUID orderId, String payloadJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("payload", payloadJson)
                .addValue("created_at", Timestamp.from(Instant.now()));
        return jdbc.update(INSERT_IF_ABSENT_SQL, params) == 1;
    }

    public List<InflightRow> fetchPendingOlderThan(Instant olderThan, int batchSize) {
        var params = new MapSqlParameterSource()
                .addValue("older_than", Timestamp.from(olderThan))
                .addValue("batch_size", batchSize);
        return jdbc.query(FETCH_PENDING_SQL, params, ROW_MAPPER);
    }

    public void markPublished(long id, Instant publishedAt) {
        jdbc.update(MARK_PUBLISHED_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("published_at", Timestamp.from(publishedAt)));
    }

    public void markFailed(long id, String error, Instant now) {
        jdbc.update(MARK_FAILED_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("error", truncateError(error))
                .addValue("now", Timestamp.from(now)));
    }

    /**
     * Slice 4p — fetch failed rows whose {@code attempts >= threshold} that have not yet been
     * compensated. Caller is the {@link com.balh.oms.reconciler.LedgerInflightHoldFailureCompensator},
     * which submits a {@code CancelOrderCommand} for each row and then calls
     * {@link #markCompensated}. {@code FOR UPDATE SKIP LOCKED} so multiple compensator JVMs in a
     * future shard topology cannot double-cancel the same order.
     */
    public List<FailedInflightRow> fetchFailedUncompensated(int threshold, int batchSize) {
        var params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("batch_size", batchSize);
        return jdbc.query(FETCH_FAILED_UNCOMPENSATED_SQL, params, FAILED_ROW_MAPPER);
    }

    public void markCompensated(long id, long cancelCorrelationId, Instant compensatedAt) {
        jdbc.update(MARK_COMPENSATED_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("cancel_correlation_id", cancelCorrelationId)
                .addValue("compensated_at", Timestamp.from(compensatedAt)));
    }

    // ---- Wed-demo: inflight lifecycle reconciler (V32) -------------------------------------

    /**
     * Persists the Ledger-returned {@code transactionId} for an existing outbox row. Idempotent:
     * never overwrites a non-null value (see SQL comment).
     *
     * @return {@code true} when the column was written by this call; {@code false} when it was
     *     already set (which is the desired idempotency outcome — caller can treat as success).
     */
    public boolean setLedgerTxnId(long id, String ledgerTxnId) {
        if (ledgerTxnId == null || ledgerTxnId.isBlank()) {
            return false;
        }
        return jdbc.update(SET_LEDGER_TXN_ID_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("ledger_txn_id", ledgerTxnId)) == 1;
    }

    public List<LifecycleSettleableRow> fetchLifecycleSettleable(
            int attemptsThreshold, int batchSize, Instant backoffFloor) {
        var params = new MapSqlParameterSource()
                .addValue("attempts_threshold", attemptsThreshold)
                .addValue("batch_size", batchSize)
                .addValue("backoff_floor", Timestamp.from(backoffFloor));
        return jdbc.query(FETCH_LIFECYCLE_SETTLEABLE_SQL, params, LIFECYCLE_ROW_MAPPER);
    }

    public void markLifecycleSettled(long id, String action, Instant settledAt) {
        jdbc.update(MARK_LIFECYCLE_SETTLED_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("action", action)
                .addValue("settled_at", Timestamp.from(settledAt)));
    }

    public void markLifecycleFailed(long id, String error, Instant now) {
        jdbc.update(MARK_LIFECYCLE_FAILED_SQL, new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("error", truncateError(error))
                .addValue("now", Timestamp.from(now)));
    }

    private static final RowMapper<LifecycleSettleableRow> LIFECYCLE_ROW_MAPPER = (rs, rowNum) -> new LifecycleSettleableRow(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getString("ledger_txn_id"),
            rs.getInt("lifecycle_attempts"),
            rs.getString("order_status"),
            rs.getInt("shard_id")
    );

    private static String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_STORED_ERROR_CHARS
                ? error
                : error.substring(0, MAX_STORED_ERROR_CHARS);
    }

    private static final RowMapper<InflightRow> ROW_MAPPER = (rs, rowNum) -> new InflightRow(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getString("payload_json"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getInt("attempts")
    );

    private static final RowMapper<FailedInflightRow> FAILED_ROW_MAPPER = (rs, rowNum) -> new FailedInflightRow(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getInt("attempts"),
            rs.getString("last_error"),
            rs.getInt("shard_id")
    );
}
