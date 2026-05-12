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
 * Postgres-backed transactional outbox for domain fanout (NATS / desk / drop copy).
 *
 * <p>Rows are inserted in the same transaction as the originating {@code orders}
 * change. {@link com.balh.oms.reconciler.DomainFanoutReconciler} delivers after commit.
 */
@Repository
public class DomainEventOutboxRepository {

    private static final int MAX_STORED_ERROR_CHARS = 4000;

    private final NamedParameterJdbcTemplate jdbc;

    public DomainEventOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record FanoutRow(long id, UUID orderId, String envelopeJson, Instant createdAt, int attempts) {}

    private static final String INSERT_SQL = """
            INSERT INTO domain_event_outbox (order_id, envelope_json, created_at)
            VALUES (:order_id, CAST(:envelope AS jsonb), :created_at)
            """;

    /**
     * {@code FOR UPDATE SKIP LOCKED} — callers must run inside a Spring transaction that spans fetch +
     * {@link #markPublished}/{@link #markFailed} (see {@link com.balh.oms.reconciler.DomainFanoutReconciler}).
     */
    private static final String FETCH_PENDING_SQL = """
            SELECT id, order_id, envelope_json::text AS envelope_json, created_at, attempts
            FROM domain_event_outbox
            WHERE published_at IS NULL
              AND created_at <= :older_than
            ORDER BY id
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_PUBLISHED_SQL = """
            UPDATE domain_event_outbox
               SET published_at = :published_at
             WHERE id = :id
            """;

    private static final String MARK_FAILED_SQL = """
            UPDATE domain_event_outbox
               SET attempts = attempts + 1,
                   last_attempt_at = :now,
                   last_error = :error
             WHERE id = :id
            """;

    public void insert(UUID orderId, String envelopeJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("envelope", envelopeJson)
                .addValue("created_at", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
    }

    public List<FanoutRow> fetchPendingOlderThan(Instant olderThan, int batchSize) {
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

    private static final RowMapper<FanoutRow> ROW_MAPPER = (rs, rowNum) -> new FanoutRow(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getString("envelope_json"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getInt("attempts")
    );
}
