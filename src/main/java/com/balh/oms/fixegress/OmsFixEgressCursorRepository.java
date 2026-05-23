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
 * separate physical table ({@code oms_fix_egress_cursor}, V26 + V56). The egress JVM advances
 * the cursor after a successful {@code Session.sendToTarget(NewOrderSingle)} so a restart
 * resumes from the last applied log position rather than re-sending every admitted order since
 * cluster boot.
 *
 * <p>This class does not perform the FIX side-effect itself — it is the cursor side-car.
 * Callers are expected to issue {@code Session.sendToTarget} <em>before</em> advancing, and to
 * tolerate at-least-once replay of the same fragment on a crash between send and advance (FIX
 * msgSeqNum and broker-side {@code DupClOrdID} handling are the protocol-level safeguards;
 * future slices add an explicit dedupe table if operational duplicate-NOS pressure exceeds the
 * broker's tolerance).
 *
 * <p><b>2026-05-23 hardening (V56).</b> The cursor now carries an explicit Aeron Archive
 * {@code last_applied_recording_id} alongside the position. Position is meaningless across
 * recording boundaries: each cluster process lifetime owns one Aeron Archive recording on the
 * events stream, and recording ids are monotonically assigned by Aeron Archive across cluster
 * restarts. Without the recording-id qualifier the egress silently fell back to recording-start
 * (position 0) on the active recording whenever the persisted cursor fell outside the active
 * recording's bounds (post-restart, the persisted cursor often pointed into a previous,
 * still-on-disk recording the egress no longer had a pointer to). The bug is identical to the
 * projector's pre-V55 bug — see
 * {@code system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md}
 * §9 and §9.6.
 *
 * <p><b>Monotonic guard for the recording-aware API.</b> {@link #advanceWithRecording} only
 * advances the cursor when {@code (recording_id, position)} is strictly greater than the saved
 * value in lexicographic order:
 *
 * <ul>
 *   <li>{@code (R, P) -> (R, P')} allowed iff {@code P' > P} — continuing within the same recording.</li>
 *   <li>{@code (R, P) -> (R', 0)} allowed iff {@code R' > R} — rolling forward to a newer
 *       recording (the egress finished one recording and is starting the next).</li>
 *   <li>{@code (R, P) -> (R', P')} rejected when {@code R' < R} — recording ids never decrease
 *       in Aeron Archive; a smaller value indicates a wiring bug or a corrupted cursor.</li>
 *   <li>{@code (NULL, P) -> anything}: the saved row has the legacy V26 shape with no recording
 *       id, and the egress refused to start (loud-fail recovery). Operators reset via
 *       {@link #resetWithRecording} or SQL before the next start.</li>
 * </ul>
 *
 * <p><b>Legacy API.</b> The position-only {@link #advance(String, int, long)} and
 * {@link #reset(String, int, long)} methods are preserved for backward compatibility with
 * pre-V56 test fixtures but are <b>no longer used</b> by {@link OmsFixEgressService} on the
 * production path; production code goes through {@link #advanceWithRecording} /
 * {@link #resetWithRecording} so the recording id moves in lockstep with the position.
 */
@Repository
public class OmsFixEgressCursorRepository {

    private static final String SELECT_POSITION_SQL = """
            SELECT last_applied_position
              FROM oms_fix_egress_cursor
             WHERE egress_id = :egress_id
               AND stream_id = :stream_id
            """;

    private static final String SELECT_CURSOR_SQL = """
            SELECT last_applied_recording_id, last_applied_position
              FROM oms_fix_egress_cursor
             WHERE egress_id = :egress_id
               AND stream_id = :stream_id
            """;

    /**
     * V60 high-water columns. Read alongside {@link #SELECT_CURSOR_SQL} when the egress'
     * init() rewind guard needs to compare the saved cursor against the historical high
     * water mark. Returning both pairs in one row avoids a second round-trip and avoids
     * the read-skew window between the two queries.
     */
    private static final String SELECT_CURSOR_AND_HIGH_WATER_SQL = """
            SELECT last_applied_recording_id, last_applied_position,
                   high_water_recording_id, high_water_position
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

    /**
     * Recording-aware upsert. Lexicographic monotonic guard on {@code (recording_id, position)}:
     * accept the new value iff its recording id is strictly higher than the saved one, OR its
     * recording id equals the saved one and the position is strictly higher. A {@code NULL}
     * saved recording id never matches under this guard — those rows must be repaired via
     * {@link #resetWithRecording} before a recording-aware caller can advance them.
     *
     * <p><b>V60 high-water bookkeeping.</b> On every successful advance the high-water columns
     * are pinned to the new {@code (recording_id, position)} too, since by construction the
     * lexicographic monotonic guard makes the new pair the new maximum. A fresh insert (no
     * prior row) seeds both pairs to the same value. The high-water columns are intentionally
     * NOT touched by {@link #RESET_WITH_RECORDING_SQL} below — that is the operator-rewind
     * affordance that {@link OmsFixEgressService#init} uses to detect rewinds.
     */
    private static final String UPSERT_WITH_RECORDING_SQL = """
            INSERT INTO oms_fix_egress_cursor
                (egress_id, stream_id,
                 last_applied_recording_id, last_applied_position,
                 high_water_recording_id, high_water_position,
                 last_applied_at)
            VALUES (:egress_id, :stream_id,
                    :last_applied_recording_id, :last_applied_position,
                    :last_applied_recording_id, :last_applied_position,
                    NOW())
            ON CONFLICT (egress_id, stream_id) DO UPDATE
               SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                   last_applied_position = EXCLUDED.last_applied_position,
                   high_water_recording_id = EXCLUDED.last_applied_recording_id,
                   high_water_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
             WHERE oms_fix_egress_cursor.last_applied_recording_id IS NOT NULL
               AND (
                     EXCLUDED.last_applied_recording_id > oms_fix_egress_cursor.last_applied_recording_id
                  OR (
                       EXCLUDED.last_applied_recording_id = oms_fix_egress_cursor.last_applied_recording_id
                       AND EXCLUDED.last_applied_position > oms_fix_egress_cursor.last_applied_position
                     )
                   )
            """;

    private static final String RESET_SQL = """
            INSERT INTO oms_fix_egress_cursor (egress_id, stream_id, last_applied_position, last_applied_at)
            VALUES (:egress_id, :stream_id, :last_applied_position, NOW())
            ON CONFLICT (egress_id, stream_id) DO UPDATE
               SET last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    /**
     * <b>Does NOT touch the V60 high-water columns.</b> That is intentional: when an operator
     * rewinds the cursor via {@code resetWithRecording(R, P)} to recover from a recording
     * trim or to re-seed bootstrap, the historical high-water mark stays in place so that
     * {@link OmsFixEgressService#init} can detect the rewind and refuse to start without
     * explicit operator acknowledgement. For a fresh insert (no prior row), the high-water
     * columns are left {@code NULL} on purpose — init() treats {@code NULL} high-water as
     * "first-ever V60 boot" and seeds it from {@code last_applied_*} after the first
     * successful advance.
     */
    private static final String RESET_WITH_RECORDING_SQL = """
            INSERT INTO oms_fix_egress_cursor
                (egress_id, stream_id, last_applied_recording_id, last_applied_position, last_applied_at)
            VALUES (:egress_id, :stream_id, :last_applied_recording_id, :last_applied_position, NOW())
            ON CONFLICT (egress_id, stream_id) DO UPDATE
               SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                   last_applied_position = EXCLUDED.last_applied_position,
                   last_applied_at = EXCLUDED.last_applied_at
            """;

    /**
     * V60 operator-explicit affordance: zero the high-water mark to acknowledge a rewind
     * decision before next start. Sets {@code high_water_recording_id} and
     * {@code high_water_position} to the values supplied (typically the same as
     * {@code last_applied_*}), bypassing the lock-step bump that {@link #UPSERT_WITH_RECORDING_SQL}
     * provides on the advance path. Never called by the egress' hot path; only invoked by
     * the operator cursor-repair script and a follow-up admin tool.
     */
    private static final String SET_HIGH_WATER_SQL = """
            UPDATE oms_fix_egress_cursor
               SET high_water_recording_id = :recording_id,
                   high_water_position = :position
             WHERE egress_id = :egress_id
               AND stream_id = :stream_id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public OmsFixEgressCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return last applied position for {@code (egressId, streamId)}, or empty if the egress
     *     has never applied.
     *
     * <p><b>Position-only view.</b> Callers that need the recording id alongside the position
     * (mandatory for safe recovery across cluster restarts — see class Javadoc and V56 migration)
     * must use {@link #findLastAppliedCursor} instead.
     */
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
     * V60 high-water-aware view. Returns the full snapshot of the cursor row needed by
     * {@link OmsFixEgressService#init} to enforce the rewind guard: {@code last_applied}
     * pair, {@code high_water} pair, plus presence flags for the legacy NULL paths
     * (pre-V56 last_applied_recording_id NULL, pre-V60 high_water_* NULL).
     */
    public Optional<RecordedCursorWithHighWater> findLastAppliedCursorWithHighWater(
            String egressId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId);
        try {
            return Optional.ofNullable(jdbc.queryForObject(
                    SELECT_CURSOR_AND_HIGH_WATER_SQL,
                    params,
                    (rs, rowNum) -> {
                        long lastRecordingId = rs.getLong(1);
                        boolean lastRecordingIdPresent = !rs.wasNull();
                        long lastPosition = rs.getLong(2);
                        long highRecordingId = rs.getLong(3);
                        boolean highRecordingIdPresent = !rs.wasNull();
                        long highPosition = rs.getLong(4);
                        boolean highPositionPresent = !rs.wasNull();
                        return new RecordedCursorWithHighWater(
                                lastRecordingIdPresent
                                        ? RecordedCursor.of(lastRecordingId, lastPosition)
                                        : RecordedCursor.legacyWithoutRecordingId(lastPosition),
                                highRecordingIdPresent && highPositionPresent
                                        ? RecordedCursor.of(highRecordingId, highPosition)
                                        : null);
                    }));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * V60 operator affordance. Sets the high-water mark to {@code (recordingId, position)},
     * typically called by the cursor-repair tool to acknowledge an operator rewind. Never
     * called on the hot path.
     */
    public void setHighWater(String egressId, int streamId, long recordingId, long position) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId)
                .addValue("recording_id", recordingId)
                .addValue("position", position);
        jdbc.update(SET_HIGH_WATER_SQL, params);
    }

    /**
     * Recording-aware view of the saved cursor. Returns empty if the row does not exist yet
     * (first-ever egress start). Returns {@link RecordedCursor#hasRecordingId()} {@code false}
     * if the row exists but predates V56 (no recording id was recorded) — recording-aware
     * callers MUST treat this as a loud-fail signal rather than silently continuing.
     */
    public Optional<RecordedCursor> findLastAppliedCursor(String egressId, int streamId) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
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
     * Advance (or insert) the cursor to {@code newPosition}. The {@code WHERE} clause on the
     * conflict path makes this a monotonic write — calling with a position {@code <=} the
     * stored value is a no-op, so retried events never roll the cursor backwards.
     *
     * <p><b>Position-only API (legacy).</b> Does not write {@code last_applied_recording_id};
     * leaves it {@code NULL} on a fresh insert. Production code in {@link OmsFixEgressService}
     * now uses {@link #advanceWithRecording} instead — this overload is kept for backward
     * compatibility with pre-V56 callers and tests.
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
     * Recording-aware advance. Monotonic guard on {@code (recording_id, position)} in
     * lexicographic order — see {@link #UPSERT_WITH_RECORDING_SQL}. Refuses to write past a
     * legacy row whose {@code last_applied_recording_id IS NULL}; the operator must
     * {@link #resetWithRecording} to repair the legacy row before recording-aware advances can
     * land.
     *
     * @return {@code true} if the cursor was inserted or advanced; {@code false} if the call
     *     was a no-op (the new pair was not strictly greater than the saved one, or the saved
     *     row had a NULL recording id).
     */
    public boolean advanceWithRecording(
            String egressId, int streamId, long recordingId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_recording_id", recordingId)
                .addValue("last_applied_position", newPosition);
        return jdbc.update(UPSERT_WITH_RECORDING_SQL, params) == 1;
    }

    /**
     * @return wall-clock {@code last_applied_at} of the egress cursor (set server-side via
     *     {@code NOW()} on every {@link #advance} / {@link #advanceWithRecording}), or empty if
     *     the egress JVM has never sent a NOS for this {@code (egressId, streamId)}. Drives the
     *     {@code oms.fix_egress.lag_seconds} gauge (slice 4d).
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
     * <p><b>Position-only API (legacy).</b> Does not touch {@code last_applied_recording_id}
     * for existing rows and does not insert one on conflict — only the position changes. New
     * rows are inserted with a {@code NULL} recording id, which is the trigger that makes the
     * loud-fail path in {@link OmsFixEgressService#init} refuse to start. Use
     * {@link #resetWithRecording} instead in new code.
     */
    public void reset(String egressId, int streamId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
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
     *   <li><b>Operator one-time repair</b> of a row stuck at
     *       {@code last_applied_recording_id IS NULL} (legacy V26 row, or a row corrupted by
     *       the pre-V56 silent-clamp bug). Operator chooses which {@code (recordingId, position)}
     *       pair represents the egress' true state and pins the cursor explicitly; subsequent
     *       recording-aware advances then run cleanly under the monotonic guard.</li>
     *   <li><b>Egress first-ever start</b> when {@link #findLastAppliedCursor} returned empty
     *       — the egress picks the oldest recording on the events stream, persists
     *       {@code (firstRecordingId, 0)} as the initial cursor, and begins replay.</li>
     * </ol>
     *
     * <p>Not used as a fallback path inside the live replay loop — the egress' loud-fail
     * design forbids that.
     */
    public void resetWithRecording(
            String egressId, int streamId, long recordingId, long newPosition) {
        var params = new MapSqlParameterSource()
                .addValue("egress_id", egressId)
                .addValue("stream_id", streamId)
                .addValue("last_applied_recording_id", recordingId)
                .addValue("last_applied_position", newPosition);
        jdbc.update(RESET_WITH_RECORDING_SQL, params);
    }

    /**
     * Saved cursor shape returned by {@link #findLastAppliedCursor}. {@link #recordingId} is
     * meaningful only when {@link #hasRecordingId()} is true; a {@code false} value indicates a
     * pre-V56 legacy row that recording-aware callers must refuse to advance from.
     */
    public record RecordedCursor(long recordingId, long position, boolean hasRecordingId) {
        public static RecordedCursor of(long recordingId, long position) {
            return new RecordedCursor(recordingId, position, true);
        }

        public static RecordedCursor legacyWithoutRecordingId(long position) {
            return new RecordedCursor(0L, position, false);
        }

        /**
         * Lexicographic order on {@code (recordingId, position)}. Both sides must have
         * {@link #hasRecordingId()} true; comparing a legacy row is a programming bug and
         * throws (init() loud-fails on legacy rows before this is ever reachable).
         */
        public int compareLex(RecordedCursor other) {
            if (!this.hasRecordingId || !other.hasRecordingId) {
                throw new IllegalStateException(
                        "compareLex called on a legacy cursor without recording id; init() must"
                                + " loud-fail on legacy rows before any comparison happens.");
            }
            int byRecording = Long.compare(this.recordingId, other.recordingId);
            return byRecording != 0 ? byRecording : Long.compare(this.position, other.position);
        }
    }

    /**
     * V60. Full snapshot of a cursor row including the high-water mark. The {@code highWater}
     * field is {@code null} when the row predates V60 — init() treats that as "first-ever V60
     * boot" and lets the next advance seed the high-water columns from {@code lastApplied}.
     */
    public record RecordedCursorWithHighWater(RecordedCursor lastApplied, RecordedCursor highWater) {
        /**
         * @return true when a non-null {@code highWater} exists and lexicographically exceeds
         *     {@code lastApplied} — the rewind signal init() acts on.
         */
        public boolean isRewound() {
            return highWater != null
                    && lastApplied.hasRecordingId()
                    && highWater.hasRecordingId()
                    && lastApplied.compareLex(highWater) < 0;
        }
    }
}
