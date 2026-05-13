package com.balh.oms.projector;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Persistence for the Aeron-log replay cursor consumed by {@link OmsPostgresProjector} and any future projector role.
 *
 * <p>Each {@code (projectorId, streamId)} pair has at most one row; the projector upserts on every applied event so a
 * crash resumes from {@code last_applied_position} on restart. See V24 migration for table semantics.
 *
 * <p>This class does not perform the projection itself — it is the cursor side car. Callers are expected to write the
 * projected row(s) and the cursor advance in the same JDBC transaction so the cursor never moves past unwritten data.
 */
@Repository
public class AeronProjectorCursorRepository {

    private static final String SELECT_POSITION_SQL = """
            SELECT last_applied_position
              FROM aeron_projector_cursor
             WHERE projector_id = :projector_id
               AND stream_id = :stream_id
            """;

    /** Slice 4d: source for the {@code oms.projector.lag_seconds} gauge. */
    private static final String SELECT_LAST_APPLIED_AT_SQL = """
            SELECT last_applied_at
              FROM aeron_projector_cursor
             WHERE projector_id = :projector_id
               AND stream_id = :stream_id
            """;

    private static final String UPSERT_SQL = """
            INSERT INTO aeron_projector_cursor (projector_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
             WHERE aeron_projector_cursor.last_applied_position < EXCLUDED.last_applied_position
            """;

    private static final String RESET_SQL = """
            INSERT INTO aeron_projector_cursor (projector_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public AeronProjectorCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return last applied position for {@code (projectorId, streamId)}, or empty if the projector has never applied.
     */
    public OptionalLong findLastAppliedPosition(String projectorId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId);
        try {
            Long pos = jdbc.queryForObject(SELECT_POSITION_SQL, params, Long.class);
            return pos == null ? OptionalLong.empty() : OptionalLong.of(pos);
        } catch (EmptyResultDataAccessException e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Advance (or insert) the cursor to {@code newPosition}. The {@code WHERE} clause on the conflict path makes this a
     * monotonic write — calling with a position {@code <=} the stored value is a no-op, so retried events never roll
     * the cursor backwards.
     *
     * @return {@code true} if the cursor was inserted or advanced; {@code false} if the call was a no-op (older or
     *         equal position).
     */
    public boolean advance(String projectorId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_position", newPosition);
        return jdbc.update(UPSERT_SQL, params) == 1;
    }

    /**
     * @return wall-clock {@code last_applied_at} of the projector cursor (set server-side via
     *     {@code NOW()} on every {@link #advance}), or empty if the projector has never applied.
     *     The time is the Postgres server's clock, not the projector JVM's — close enough for the
     *     {@code oms.projector.lag_seconds} gauge (slice 4d) since lag-budget alerts work on
     *     order-of-seconds granularity and any per-host clock skew is dwarfed by the lag we care
     *     about.
     */
    public Optional<Instant> findLastAppliedAt(String projectorId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId);
        try {
            Timestamp ts = jdbc.queryForObject(SELECT_LAST_APPLIED_AT_SQL, params, Timestamp.class);
            return ts == null ? Optional.empty() : Optional.of(ts.toInstant());
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Convenience wrapper used by tests and ops queries.
     */
    public Optional<Long> findLastAppliedPositionBoxed(String projectorId, int streamId) {
        OptionalLong opt = findLastAppliedPosition(projectorId, streamId);
        return opt.isPresent() ? Optional.of(opt.getAsLong()) : Optional.empty();
    }

    /**
     * Force the cursor to {@code newPosition}, bypassing the monotonic guard used by {@link #advance}.
     *
     * <p>Reserved for the projector's <em>recording-clamp</em> path: when the persisted cursor is
     * outside the current recording's {@code [startPosition, stopPosition]} range — typically after
     * the recording has been recreated — the projector resets the cursor to the recording's
     * {@code startPosition} so replay can resume against the new recording. This is the only place
     * the cursor moves backwards. Callers must guarantee the new position is consistent with the
     * recording it pairs with; otherwise downstream events will be re-applied against an idempotent
     * Postgres write (which is fine) but observed cursors lose their monotonicity property.
     */
    public void reset(String projectorId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_position", newPosition);
        jdbc.update(RESET_SQL, params);
    }
}
