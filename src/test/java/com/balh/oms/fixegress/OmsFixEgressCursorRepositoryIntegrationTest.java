package com.balh.oms.fixegress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 3b-1 coverage: V26 migration applied, cursor repository upserts and reads cleanly,
 * the monotonic {@code WHERE last_applied_position &lt; EXCLUDED.last_applied_position}
 * clause is enforced, and {@link OmsFixEgressCursorRepository#reset} bypasses that guard for
 * the recording-clamp path.
 *
 * <p>2026-05-23 hardening (V56) extends this with recording-aware coverage: lexicographic
 * monotonic guard on {@code (recording_id, position)}, legacy-NULL-row blocking by
 * {@code advanceWithRecording}, and unconditional repair via {@code resetWithRecording}.
 * Mirrors {@code AeronProjectorCursorRepositoryIntegrationTest}'s recording-aware test block
 * to keep the two cursor surfaces in lockstep.
 */
class OmsFixEgressCursorRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final String EGRESS_ID = "test-egress";
    private static final int STREAM_ID = 42;

    @Autowired
    OmsFixEgressCursorRepository cursorRepository;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanCursor() {
        jdbc.update("DELETE FROM oms_fix_egress_cursor WHERE egress_id = ?", EGRESS_ID);
    }

    @Test
    void noPriorRow_returnsEmpty() {
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void firstAdvance_inserts() {
        boolean changed = cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        assertThat(changed).isTrue();
        OptionalLong pos = cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID);
        assertThat(pos).hasValue(100L);
    }

    @Test
    void monotonicAdvance_updatesForwards_noopForOlder() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);

        boolean forwards = cursorRepository.advance(EGRESS_ID, STREAM_ID, 200L);
        assertThat(forwards).isTrue();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);

        boolean older = cursorRepository.advance(EGRESS_ID, STREAM_ID, 150L);
        assertThat(older).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);

        boolean equal = cursorRepository.advance(EGRESS_ID, STREAM_ID, 200L);
        assertThat(equal).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(200L);
    }

    @Test
    void streamsAreIndependent() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        cursorRepository.advance(EGRESS_ID, STREAM_ID + 1, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID + 1)).hasValue(50L);
    }

    @Test
    void reset_bypassesMonotonicGuard() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 500L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(500L);

        boolean rewindByAdvance = cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        assertThat(rewindByAdvance).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(500L);

        cursorRepository.reset(EGRESS_ID, STREAM_ID, 100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
    }

    @Test
    void egressIdsAreIndependent() {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        cursorRepository.advance(EGRESS_ID + "-other", STREAM_ID, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(EGRESS_ID + "-other", STREAM_ID))
                .hasValue(50L);

        jdbc.update("DELETE FROM oms_fix_egress_cursor WHERE egress_id = ?", EGRESS_ID + "-other");
    }

    @Test
    void findLastAppliedAt_emptyBeforeFirstAdvance_thenTracksAdvance() {
        assertThat(cursorRepository.findLastAppliedAt(EGRESS_ID, STREAM_ID)).isEmpty();

        Instant before = Instant.now();
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        Instant after = Instant.now();

        Optional<Instant> ts = cursorRepository.findLastAppliedAt(EGRESS_ID, STREAM_ID);
        assertThat(ts).isPresent();
        assertThat(ts.get()).isBetween(before.minus(Duration.ofSeconds(2)), after.plus(Duration.ofSeconds(2)));
    }

    @Test
    void findLastAppliedAt_advancesOnUpsert_butNotOnNoopOlderWrite() throws InterruptedException {
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 200L);
        Optional<Instant> firstTs = cursorRepository.findLastAppliedAt(EGRESS_ID, STREAM_ID);
        assertThat(firstTs).isPresent();

        Thread.sleep(50);

        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);
        Optional<Instant> staleTs = cursorRepository.findLastAppliedAt(EGRESS_ID, STREAM_ID);
        assertThat(staleTs).contains(firstTs.get());

        Thread.sleep(50);

        cursorRepository.advance(EGRESS_ID, STREAM_ID, 300L);
        Optional<Instant> bumpedTs = cursorRepository.findLastAppliedAt(EGRESS_ID, STREAM_ID);
        assertThat(bumpedTs).isPresent();
        assertThat(bumpedTs.get()).isAfter(firstTs.get());
    }

    // ========================================================================
    // 2026-05-23 hardening (V56): recording-aware cursor API. Mirrors the
    // AeronProjectorCursorRepositoryIntegrationTest §V55 block one-for-one.
    // ========================================================================

    @Test
    void findLastAppliedCursor_emptyWhenNoRow() {
        assertThat(cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void findLastAppliedCursor_legacyRow_reportsHasRecordingIdFalse() {
        // Legacy V26 callers use advance() which leaves last_applied_recording_id NULL.
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);

        Optional<OmsFixEgressCursorRepository.RecordedCursor> cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID);
        assertThat(cursor).isPresent();
        assertThat(cursor.get().hasRecordingId()).isFalse();
        assertThat(cursor.get().position()).isEqualTo(100L);
    }

    @Test
    void advanceWithRecording_firstWrite_inserts() {
        boolean changed = cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 1000L);
        assertThat(changed).isTrue();

        Optional<OmsFixEgressCursorRepository.RecordedCursor> cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID);
        assertThat(cursor).isPresent();
        assertThat(cursor.get().hasRecordingId()).isTrue();
        assertThat(cursor.get().recordingId()).isEqualTo(13L);
        assertThat(cursor.get().position()).isEqualTo(1000L);
    }

    @Test
    void advanceWithRecording_sameRecording_advancesForward_noopBackwards() {
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 1000L);

        assertThat(cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 2000L)).isTrue();
        assertThat(cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get().position())
                .isEqualTo(2000L);

        assertThat(cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 1500L)).isFalse();
        assertThat(cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get().position())
                .isEqualTo(2000L);

        assertThat(cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 2000L)).isFalse();
        assertThat(cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get().position())
                .isEqualTo(2000L);
    }

    @Test
    void advanceWithRecording_newerRecording_winsEvenWithSmallerPosition() {
        // Cluster restart promotes the cursor to a new recording at position 0 — this MUST be
        // allowed even though position(0) < saved position(2000). Without this, the egress
        // could never roll forward to a freshly opened recording after a cluster restart.
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 2000L);

        boolean rolledForward = cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 0L);
        assertThat(rolledForward).isTrue();

        OmsFixEgressCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(0L);
    }

    @Test
    void advanceWithRecording_olderRecording_rejected_evenWithLargerPosition() {
        // Aeron Archive recording ids never decrease. A write with a smaller recording id is a
        // wiring bug (egress pointed at the wrong Archive) or a corruption — must NOT silently
        // overwrite a saved newer-recording cursor.
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 500L);

        boolean rejected = cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 999_999L);
        assertThat(rejected).isFalse();

        OmsFixEgressCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(500L);
    }

    @Test
    void advanceWithRecording_legacyNullRecordingRow_doesNotOverwrite() {
        // The pre-V56 row has last_applied_recording_id NULL. A recording-aware advance must
        // refuse to overwrite it silently — the egress' init() path requires operator
        // intervention to repair via resetWithRecording first.
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 100L);

        boolean blocked = cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 13L, 999L);
        assertThat(blocked).isFalse();

        OmsFixEgressCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get();
        assertThat(cursor.hasRecordingId()).isFalse();
        assertThat(cursor.position()).isEqualTo(100L);
    }

    @Test
    void resetWithRecording_overwritesUnconditionally_includingBackwards() {
        // Operator-driven repair must be able to land any (recordingId, position) — including
        // moving the cursor backwards if the operator has determined the saved state was wrong.
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 5_000L);

        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 13L, 100L);

        OmsFixEgressCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(13L);
        assertThat(cursor.position()).isEqualTo(100L);
    }

    @Test
    void resetWithRecording_repairsLegacyNullRow() {
        // The pop 2026-05-23 recovery flow (egress-side mirror of the projector V55 flow):
        // operator runs resetWithRecording to populate the missing recording id on a legacy
        // row, then the egress can start.
        cursorRepository.advance(EGRESS_ID, STREAM_ID, 42_464L);

        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 16L, 0L);

        OmsFixEgressCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(EGRESS_ID, STREAM_ID).get();
        assertThat(cursor.hasRecordingId()).isTrue();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(0L);

        // After the repair, a recording-aware advance lands cleanly.
        assertThat(cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 100L)).isTrue();
    }

    // --------------------------------------------------------------------------------------
    // V60 high-water-mark coverage (gap 2 of the post-V55/V56 stability review).
    // --------------------------------------------------------------------------------------

    @Test
    void v60_advanceWithRecording_bumpsHighWaterInLockstep() {
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 1_000L);
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 2_000L);
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 17L, 0L);

        OmsFixEgressCursorRepository.RecordedCursorWithHighWater row =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(row.lastApplied().recordingId()).isEqualTo(17L);
        assertThat(row.lastApplied().position()).isEqualTo(0L);
        assertThat(row.highWater()).isNotNull();
        assertThat(row.highWater().recordingId()).isEqualTo(17L);
        assertThat(row.highWater().position()).isEqualTo(0L);
        assertThat(row.isRewound()).isFalse();
    }

    @Test
    void v60_resetWithRecording_doesNotTouchHighWater_so_rewindIsDetectable() {
        // Steady-state: advance lockstep-bumps both pairs.
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 5_000L);

        // Operator rewinds via resetWithRecording. last_applied moves backwards; high_water
        // stays in place. This is the breadcrumb that lets init() detect the rewind.
        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 16L, 100L);

        OmsFixEgressCursorRepository.RecordedCursorWithHighWater row =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(row.lastApplied().recordingId()).isEqualTo(16L);
        assertThat(row.lastApplied().position()).isEqualTo(100L);
        assertThat(row.highWater().recordingId()).isEqualTo(16L);
        assertThat(row.highWater().position()).isEqualTo(5_000L);
        assertThat(row.isRewound()).isTrue();
    }

    @Test
    void v60_resetWithRecording_rewindToLowerRecordingId_isDetectable() {
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 0L);
        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 12L, 0L);

        OmsFixEgressCursorRepository.RecordedCursorWithHighWater row =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(row.lastApplied().recordingId()).isEqualTo(12L);
        assertThat(row.highWater().recordingId()).isEqualTo(16L);
        assertThat(row.isRewound()).isTrue();
    }

    @Test
    void v60_setHighWater_acknowledgesRewind_isRewoundReturnsFalse() {
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 16L, 5_000L);
        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 12L, 0L);

        // Operator acknowledges the rewind by zeroing the high-water mark.
        cursorRepository.setHighWater(EGRESS_ID, STREAM_ID, 12L, 0L);

        OmsFixEgressCursorRepository.RecordedCursorWithHighWater row =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(row.lastApplied().recordingId()).isEqualTo(12L);
        assertThat(row.lastApplied().position()).isEqualTo(0L);
        assertThat(row.highWater().recordingId()).isEqualTo(12L);
        assertThat(row.highWater().position()).isEqualTo(0L);
        assertThat(row.isRewound()).isFalse();
    }

    @Test
    void v60_resetWithRecording_freshInsert_leavesHighWaterNull_treatedAsFirstStart() {
        // Fresh insert via resetWithRecording (operator bootstrap path or first-ever start
        // taken by the egress' bootstrapFromOldestRecording). High-water columns stay NULL;
        // init() must treat that as "first-ever V60 boot, not a rewind".
        cursorRepository.resetWithRecording(EGRESS_ID, STREAM_ID, 12L, 0L);

        OmsFixEgressCursorRepository.RecordedCursorWithHighWater row =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(row.lastApplied().recordingId()).isEqualTo(12L);
        assertThat(row.highWater()).isNull();
        assertThat(row.isRewound()).isFalse();

        // The next successful advance lockstep-seeds the high-water columns.
        cursorRepository.advanceWithRecording(EGRESS_ID, STREAM_ID, 12L, 100L);
        OmsFixEgressCursorRepository.RecordedCursorWithHighWater after =
                cursorRepository.findLastAppliedCursorWithHighWater(EGRESS_ID, STREAM_ID).get();
        assertThat(after.lastApplied().position()).isEqualTo(100L);
        assertThat(after.highWater()).isNotNull();
        assertThat(after.highWater().recordingId()).isEqualTo(12L);
        assertThat(after.highWater().position()).isEqualTo(100L);
    }

    @Test
    void v60_recordedCursor_compareLex_legacyRow_throws() {
        OmsFixEgressCursorRepository.RecordedCursor legacy =
                OmsFixEgressCursorRepository.RecordedCursor.legacyWithoutRecordingId(100L);
        OmsFixEgressCursorRepository.RecordedCursor normal =
                OmsFixEgressCursorRepository.RecordedCursor.of(16L, 100L);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> legacy.compareLex(normal));
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> normal.compareLex(legacy));
    }
}
