package com.balh.oms.projector;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

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

    private static final String UPSERT_SQL = """
            INSERT INTO aeron_projector_cursor (projector_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
             WHERE aeron_projector_cursor.last_applied_position < EXCLUDED.last_applied_position
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
     * Convenience wrapper used by tests and ops queries.
     */
    public Optional<Long> findLastAppliedPositionBoxed(String projectorId, int streamId) {
        OptionalLong opt = findLastAppliedPosition(projectorId, streamId);
        return opt.isPresent() ? Optional.of(opt.getAsLong()) : Optional.empty();
    }
}
