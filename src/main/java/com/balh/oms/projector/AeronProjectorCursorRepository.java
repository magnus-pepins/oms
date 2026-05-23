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
 * <p><b>2026-05-23 hardening (V55).</b> The cursor now carries an explicit Aeron Archive
 * {@code last_applied_recording_id} alongside the position. Position is meaningless across
 * recording boundaries: each cluster process lifetime owns one Aeron Archive recording on the
 * events stream, and recording ids are monotonically assigned by Aeron Archive across cluster
 * restarts. Without the recording-id qualifier the projector silently lost its pointer to
 * pre-restart events when the cluster restarted (pop 2026-05-23 — projector ended up reading
 * recording 16 from position 0 with 9 admitted orders invisible because their admit events lived
 * in recording 13). See
 * {@code system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md}
 * §9 for the full incident narrative.
 *
 * <p><b>Monotonic guard for the recording-aware API.</b> {@link #advanceWithRecording} only
 * advances the cursor when {@code (recording_id, position)} is strictly greater than the saved
 * value in lexicographic order:
 *
 * <ul>
 *   <li>{@code (R, P) -> (R, P')} allowed iff {@code P' > P} — continuing within the same recording.</li>
 *   <li>{@code (R, P) -> (R', 0)} allowed iff {@code R' > R} — rolling forward to a newer
 *       recording (the projector finished one recording and is starting the next).</li>
 *   <li>{@code (R, P) -> (R', P')} rejected when {@code R' < R} — recording ids never decrease
 *       in Aeron Archive; a smaller value indicates a wiring bug or a corrupted cursor.</li>
 *   <li>{@code (NULL, P) -> anything}: the saved row has the legacy V24 shape with no recording id,
 *       and the projector refused to start (loud-fail recovery). Operators reset via
 *       {@link #resetWithRecording} or SQL before the next start.</li>
 * </ul>
 *
 * <p>This class does not perform the projection itself — it is the cursor side car. Callers are expected to write the
 * projected row(s) and the cursor advance in the same JDBC transaction so the cursor never moves past unwritten data.
 *
 * <p><b>Legacy API.</b> The position-only {@link #advance(String, int, long)} and
 * {@link #reset(String, int, long)} methods are preserved unchanged for {@code OmsFixEgressService}
 * (a sibling consumer of this table) which has the same latent cursor-recording bug but is not yet
 * migrated. Both old and new methods coexist on the same row: legacy callers leave
 * {@code last_applied_recording_id} {@code NULL}, recording-aware callers populate it. The two
 * worlds don't share rows because their {@code projector_id} differs.
 */
@Repository
public class AeronProjectorCursorRepository {

    private static final String SELECT_POSITION_SQL = """
            SELECT last_applied_position
              FROM aeron_projector_cursor
             WHERE projector_id = :projector_id
               AND stream_id = :stream_id
            """;

    private static final String SELECT_CURSOR_SQL = """
            SELECT last_applied_recording_id, last_applied_position
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

    /**
     * Recording-aware upsert. Lexicographic monotonic guard on {@code (recording_id, position)}:
     * accept the new value iff its recording id is strictly higher than the saved one, OR its
     * recording id equals the saved one and the position is strictly higher. A {@code NULL}
     * saved recording id never matches under this guard — those rows must be repaired via
     * {@link #resetWithRecording} before a recording-aware caller can advance them.
     */
    private static final String UPSERT_WITH_RECORDING_SQL = """
            INSERT INTO aeron_projector_cursor
                (projector_id, stream_id, last_applied_recording_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_recording_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                   last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
             WHERE aeron_projector_cursor.last_applied_recording_id IS NOT NULL
               AND (
                     EXCLUDED.last_applied_recording_id > aeron_projector_cursor.last_applied_recording_id
                  OR (
                       EXCLUDED.last_applied_recording_id = aeron_projector_cursor.last_applied_recording_id
                       AND EXCLUDED.last_applied_position > aeron_projector_cursor.last_applied_position
                     )
                   )
            """;

    private static final String RESET_SQL = """
            INSERT INTO aeron_projector_cursor (projector_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    private static final String RESET_WITH_RECORDING_SQL = """
            INSERT INTO aeron_projector_cursor
                (projector_id, stream_id, last_applied_recording_id, last_applied_position, last_applied_at)
            VALUES (:projector_id, :stream_id, :last_applied_recording_id, :last_applied_position, NOW())
            ON CONFLICT (projector_id, stream_id) DO UPDATE
               SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                   last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public AeronProjectorCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return last applied position for {@code (projectorId, streamId)}, or empty if the projector has never applied.
     *
     * <p><b>Position-only view.</b> Callers that need the recording id alongside the position
     * (mandatory for safe recovery across cluster restarts — see class Javadoc and V55 migration)
     * must use {@link #findLastAppliedCursor} instead.
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
     * Recording-aware view of the saved cursor. Returns empty if the row does not exist yet
     * (first-ever projector start). Returns {@link RecordedCursor#hasRecordingId()} {@code false}
     * if the row exists but predates V55 (no recording id was recorded) — recording-aware callers
     * MUST treat this as a loud-fail signal rather than silently continuing.
     */
    public Optional<RecordedCursor> findLastAppliedCursor(String projectorId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId);
        try {
            return Optional.ofNullable(jdbc.queryForObject(
                    SELECT_CURSOR_SQL,
                    params,
                    (rs, rowNum) -> {
                        long recordingId = rs.getLong(1);
                        boolean recordingIdPresent = !rs.wasNull();
                        long position = rs.getLong(2);
                        return recordingIdPresent
                                ? RecordedCursor.of(recordingId, position)
                                : RecordedCursor.legacyWithoutRecordingId(position);
                    }));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Advance (or insert) the cursor to {@code newPosition}. The {@code WHERE} clause on the conflict path makes this a
     * monotonic write — calling with a position {@code <=} the stored value is a no-op, so retried events never roll
     * the cursor backwards.
     *
     * <p><b>Position-only API (legacy).</b> Does not write {@code last_applied_recording_id};
     * leaves it {@code NULL} on a fresh insert. Used by {@code OmsFixEgressService} which has the
     * same latent cluster-restart bug as the projector but is not yet migrated to the
     * recording-aware path. Do not use for new code.
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
     * Recording-aware advance. Monotonic guard on {@code (recording_id, position)} in
     * lexicographic order — see {@link #UPSERT_WITH_RECORDING_SQL}. Refuses to write past a
     * legacy row whose {@code last_applied_recording_id IS NULL}; the operator must
     * {@link #resetWithRecording} to repair the legacy row before recording-aware advances can
     * land.
     *
     * @return {@code true} if the cursor was inserted or advanced; {@code false} if the call was
     *         a no-op (the new pair was not strictly greater than the saved one, or the saved row
     *         had a NULL recording id).
     */
    public boolean advanceWithRecording(
            String projectorId, int streamId, long recordingId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_recording_id", recordingId)
                .addValue("last_applied_position", newPosition);
        return jdbc.update(UPSERT_WITH_RECORDING_SQL, params) == 1;
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
     * <p><b>Position-only API (legacy).</b> Does not touch {@code last_applied_recording_id} for
     * existing rows and does not insert one on conflict — only the position changes. New rows
     * are inserted with a {@code NULL} recording id. Reserved for legacy
     * {@code OmsFixEgressService} use and for the projector's recording-clamp fallback in the
     * pre-V55 code path that no longer runs after the 2026-05-23 hardening.
     *
     * <p>The recording-aware projector recovery uses {@link #resetWithRecording} instead so the
     * recording id moves in lockstep with the position.
     */
    public void reset(String projectorId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_position", newPosition);
        jdbc.update(RESET_SQL, params);
    }

    /**
     * Force the cursor to {@code (recordingId, newPosition)}, bypassing the monotonic guard.
     *
     * <p>Two intended uses, both deliberately not silent:
     *
     * <ol>
     *   <li><b>Operator one-time repair</b> of a row stuck at {@code last_applied_recording_id IS NULL}
     *       (legacy V24 row, or a row corrupted by the pre-V55 silent-clamp bug). Operator
     *       chooses which {@code (recordingId, position)} pair represents the projector's true
     *       state and pins the cursor explicitly; subsequent recording-aware advances then run
     *       cleanly under the monotonic guard.</li>
     *   <li><b>Projector first-ever start</b> when {@link #findLastAppliedCursor} returned empty
     *       — the projector picks the oldest recording on the events stream, persists
     *       {@code (firstRecordingId, 0)} as the initial cursor, and begins replay.</li>
     * </ol>
     *
     * <p>Not used as a fallback path inside the live replay loop — the projector's loud-fail
     * design forbids that. See class Javadoc + V55 migration comment.
     */
    public void resetWithRecording(
            String projectorId, int streamId, long recordingId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("projector_id", projectorId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_recording_id", recordingId)
                .addValue("last_applied_position", newPosition);
        jdbc.update(RESET_WITH_RECORDING_SQL, params);
    }

    /**
     * Saved cursor shape returned by {@link #findLastAppliedCursor}. {@link #recordingId} is
     * meaningful only when {@link #hasRecordingId()} is true; a {@code false} value indicates a
     * pre-V55 legacy row that recording-aware callers must refuse to advance from.
     */
    public record RecordedCursor(long recordingId, long position, boolean hasRecordingId) {
        public static RecordedCursor of(long recordingId, long position) {
            return new RecordedCursor(recordingId, position, true);
        }

        public static RecordedCursor legacyWithoutRecordingId(long position) {
            return new RecordedCursor(0L, position, false);
        }
    }
}
