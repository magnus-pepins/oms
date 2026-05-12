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

    private static final String INSERT_SQL = """
            INSERT INTO ledger_inflight_outbox (order_id, payload_json, created_at)
            VALUES (:order_id, CAST(:payload AS jsonb), :created_at)
            """;

    /**
     * {@code FOR UPDATE SKIP LOCKED} — callers must run inside a Spring transaction that spans fetch +
     * {@link #markPublished}/{@link #markFailed} (see {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler}).
     */
    private static final String FETCH_PENDING_SQL = """
            SELECT id, order_id, payload_json::text AS payload_json, created_at, attempts
            FROM ledger_inflight_outbox
            WHERE published_at IS NULL
              AND created_at <= :older_than
            ORDER BY id
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
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

    public void insert(UUID orderId, String payloadJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("payload", payloadJson)
                .addValue("created_at", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
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
}
