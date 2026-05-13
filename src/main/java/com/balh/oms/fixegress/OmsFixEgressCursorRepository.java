package com.balh.oms.fixegress;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Persistence for the Aeron-log replay cursor consumed by {@link OmsFixEgressService}.
 *
 * <p>Mirrors {@link com.balh.oms.projector.AeronProjectorCursorRepository} shape-for-shape on a
 * separate physical table ({@code oms_fix_egress_cursor}, V26). The egress JVM advances the
 * cursor after a successful {@code Session.sendToTarget(NewOrderSingle)} so a restart resumes
 * from the last applied log position rather than re-sending every admitted order since cluster
 * boot. {@link #advance} is monotonic (the conflict-path WHERE clause guards against a stale
 * write rolling the cursor backwards); {@link #reset} bypasses that guard and is reserved for
 * the recording-clamp / recording-recreation path (slice 3b — same shape as the projector).
 *
 * <p>This class does not perform the FIX side-effect itself — it is the cursor side-car.
 * Callers are expected to issue {@code Session.sendToTarget} <em>before</em> advancing, and to
 * tolerate at-least-once replay of the same fragment on a crash between send and advance (FIX
 * msgSeqNum and broker-side {@code DupClOrdID} handling are the protocol-level safeguards;
 * future slices add an explicit dedupe table if operational duplicate-NOS pressure exceeds the
 * broker's tolerance).
 */
@Repository
public class OmsFixEgressCursorRepository {

    private static final String SELECT_POSITION_SQL = """
            SELECT last_applied_position
              FROM oms_fix_egress_cursor
             WHERE egress_id = :egress_id
               AND stream_id = :stream_id
            """;

    /** Slice 4d: source for the {@code oms.fix_egress.lag_seconds} gauge. */
    private static final String SELECT_LAST_APPLIED_AT_SQL = """
            SELECT last_applied_at
              FROM oms_fix_egress_cursor
             WHERE egress_id = :egress_id
               AND stream_id = :stream_id
            """;

    private static final String UPSERT_SQL = """
            INSERT INTO oms_fix_egress_cursor (egress_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:egress_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (egress_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
             WHERE oms_fix_egress_cursor.last_applied_position < EXCLUDED.last_applied_position
            """;

    private static final String RESET_SQL = """
            INSERT INTO oms_fix_egress_cursor (egress_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:egress_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (egress_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public OmsFixEgressCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public OptionalLong findLastAppliedPosition(String egressId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId);
        try {
            Long pos = jdbc.queryForObject(SELECT_POSITION_SQL, params, Long.class);
            return pos == null ? OptionalLong.empty() : OptionalLong.of(pos);
        } catch (EmptyResultDataAccessException e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Advance (or insert) the cursor to {@code newPosition}. The {@code WHERE} clause on the
     * conflict path makes this a monotonic write — calling with a position {@code <=} the
     * stored value is a no-op, so retried events never roll the cursor backwards.
     *
     * @return {@code true} if the cursor was inserted or advanced; {@code false} if the call
     *     was a no-op (older or equal position).
     */
    public boolean advance(String egressId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_position", newPosition);
        return jdbc.update(UPSERT_SQL, params) == 1;
    }

    /**
     * @return wall-clock {@code last_applied_at} of the egress cursor (set server-side via
     *     {@code NOW()} on every {@link #advance}), or empty if the egress JVM has never sent
     *     a NOS for this {@code (egressId, streamId)}. Drives the {@code oms.fix_egress.lag_seconds}
     *     gauge (slice 4d).
     */
    public Optional<Instant> findLastAppliedAt(String egressId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId);
        try {
            Timestamp ts = jdbc.queryForObject(SELECT_LAST_APPLIED_AT_SQL, params, Timestamp.class);
            return ts == null ? Optional.empty() : Optional.of(ts.toInstant());
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<Long> findLastAppliedPositionBoxed(String egressId, int streamId) {
        OptionalLong opt = findLastAppliedPosition(egressId, streamId);
        return opt.isPresent() ? Optional.of(opt.getAsLong()) : Optional.empty();
    }

    /**
     * Force the cursor to {@code newPosition}, bypassing the monotonic guard used by
     * {@link #advance}.
     *
     * <p>Reserved for the egress' <em>recording-clamp</em> path: when the persisted cursor is
     * outside the current recording's {@code [startPosition, stopPosition]} range — typically
     * after the recording has been recreated (test JVM reboot, prod node disk wipe, manual
     * archive deletion) — the egress resets the cursor to the recording's
     * {@code startPosition} so replay can resume against the new recording. This is the only
     * place the cursor moves backwards. Callers must guarantee the new position is consistent
     * with the recording it pairs with; otherwise downstream events will be re-applied (which
     * is fine for the cursor itself, but each replay re-sends NOS, so operators must prefer a
     * deliberate reset over silent rewinding).
     */
    public void reset(String egressId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_position", newPosition);
        jdbc.update(RESET_SQL, params);
    }
}
