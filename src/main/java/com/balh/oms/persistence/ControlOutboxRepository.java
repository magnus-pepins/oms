package com.balh.oms.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Postgres-backed repository for {@code control_outbox}.
 *
 * <p>{@link #insert(UUID, int, String, Instant)} runs inside the same transaction as the
 * {@code orders} insert/update. Only after Postgres commits does {@code OutboxReconciler}
 * append the row to the control journal and {@link #markAppended(long, Instant)} close the loop.
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
            RETURNING id
            """;

    /**
     * {@code FOR UPDATE SKIP LOCKED} so multiple {@link com.balh.oms.reconciler.OutboxReconciler} JVMs (or ticks)
     * cannot claim the same pending row in one transaction; callers must invoke from an active Spring transaction.
     */
    private static final String FETCH_PENDING_SQL = """
            SELECT id, order_id, order_version, payload::text AS payload,
                   enqueued_at, attempts
            FROM control_outbox
            WHERE chronicle_enqueued_at IS NULL
              AND enqueued_at <= :older_than
            ORDER BY id
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
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

    public record InsertResult(long id, Instant enqueuedAt) {}

    public InsertResult insert(UUID orderId, int orderVersion, String payload, Instant enqueuedAt) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("order_version", orderVersion)
                .addValue("payload", payload)
                .addValue("enqueued_at", Timestamp.from(enqueuedAt));
        Long id = jdbc.queryForObject(INSERT_SQL, params, Long.class);
        return new InsertResult(Objects.requireNonNull(id), enqueuedAt);
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
