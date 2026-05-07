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
 * Postgres-backed repository for {@code control_outbox}.
 *
 * <p>{@link #insert(UUID, int, String)} runs inside the same transaction as the
 * {@code orders} insert/update. Only after Postgres commits does the
 * {@code OutboxReconciler} pick up rows where {@code chronicle_enqueued_at IS NULL}
 * and append them to Chronicle. {@link #markAppended(long, Instant)} closes the
 * loop.
 */
@Repository
public class ControlOutboxRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ControlOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record OutboxRow(
            long id,
            UUID orderId,
            int orderVersion,
            String payload,
            Instant enqueuedAt,
            int attempts
    ) {}

    private static final String INSERT_SQL = """
            INSERT INTO control_outbox (order_id, order_version, payload, enqueued_at)
            VALUES (:order_id, :order_version, CAST(:payload AS jsonb), :enqueued_at)
            """;

    private static final String FETCH_PENDING_SQL = """
            SELECT id, order_id, order_version, payload::text AS payload,
                   enqueued_at, attempts
            FROM control_outbox
            WHERE chronicle_enqueued_at IS NULL
              AND enqueued_at <= :older_than
            ORDER BY id
            LIMIT :batch_size
            """;

    private static final String MARK_APPENDED_SQL = """
            UPDATE control_outbox
               SET chronicle_enqueued_at = :appended_at
             WHERE id = :id
            """;

    private static final String MARK_FAILED_SQL = """
            UPDATE control_outbox
               SET attempts = attempts + 1,
                   last_attempt_at = :now,
                   last_error = :error
             WHERE id = :id
            """;

    public void insert(UUID orderId, int orderVersion, String payload) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("order_version", orderVersion)
                .addValue("payload", payload)
                .addValue("enqueued_at", Timestamp.from(Instant.now()));
        jdbc.update(INSERT_SQL, params);
    }

    public List<OutboxRow> fetchPendingOlderThan(Instant olderThan, int batchSize) {
        var params = new MapSqlParameterSource()
                .addValue("older_than", Timestamp.from(olderThan))
                .addValue("batch_size", batchSize);
        return jdbc.query(FETCH_PENDING_SQL, params, ROW_MAPPER);
    }

    public void markAppended(long id, Instant appendedAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("appended_at", Timestamp.from(appendedAt));
        jdbc.update(MARK_APPENDED_SQL, params);
    }

    public void markFailed(long id, String error) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("now", Timestamp.from(Instant.now()))
                .addValue("error", error);
        jdbc.update(MARK_FAILED_SQL, params);
    }

    private static final RowMapper<OutboxRow> ROW_MAPPER = (rs, rowNum) -> new OutboxRow(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getInt("order_version"),
            rs.getString("payload"),
            rs.getTimestamp("enqueued_at").toInstant(),
            rs.getInt("attempts")
    );
}
