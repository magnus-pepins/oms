package com.balh.oms.projector;

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
 * Slice 2a coverage: V24 migration applied, cursor repository upserts and reads cleanly, and the monotonic
 * {@code WHERE last_applied_position &lt; EXCLUDED.last_applied_position} clause is enforced.
 */
class AeronProjectorCursorRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final String PROJECTOR_ID = "test-projector";
    private static final int STREAM_ID = 42;

    @Autowired
    AeronProjectorCursorRepository cursorRepository;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanCursor() {
        jdbc.update("DELETE FROM aeron_projector_cursor WHERE projector_id = ?", PROJECTOR_ID);
    }

    @Test
    void noPriorRow_returnsEmpty() {
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void firstAdvance_inserts() {
        boolean changed = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        assertThat(changed).isTrue();
        OptionalLong pos = cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID);
        assertThat(pos).hasValue(100L);
    }

    @Test
    void monotonicAdvance_updatesForwards_noopForOlder() {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);

        boolean forwards = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        assertThat(forwards).isTrue();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);

        boolean older = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 150L);
        assertThat(older).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);

        boolean equal = cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        assertThat(equal).isFalse();
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(200L);
    }

    @Test
    void streamsAreIndependent() {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID + 1, 50L);

        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID)).hasValue(100L);
        assertThat(cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID + 1)).hasValue(50L);
    }

    @Test
    void findLastAppliedAt_emptyBeforeFirstAdvance_thenTracksAdvance() {
        assertThat(cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID)).isEmpty();

        Instant before = Instant.now();
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        Instant after = Instant.now();

        Optional<Instant> ts = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(ts).isPresent();
        assertThat(ts.get()).isBetween(before.minus(Duration.ofSeconds(2)), after.plus(Duration.ofSeconds(2)));
    }

    // ---------------------------------------------------------------------------------------------
    // 2026-05-23 hardening (V55 migration + advanceWithRecording / resetWithRecording / findLastAppliedCursor).
    // Each cluster restart opens a new Aeron Archive recording on the events stream; the cursor
    // must carry the recording id alongside the position so projector recovery can walk from the
    // saved (recording_id, position) into newer recordings without silently losing visibility
    // into prior cluster incarnations. See repository class Javadoc + handover §9.
    // ---------------------------------------------------------------------------------------------

    @Test
    void findLastAppliedCursor_emptyWhenNoRow() {
        assertThat(cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID)).isEmpty();
    }

    @Test
    void findLastAppliedCursor_legacyRow_reportsHasRecordingIdFalse() {
        // Legacy V24 callers use advance() which leaves last_applied_recording_id NULL.
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);

        Optional<AeronProjectorCursorRepository.RecordedCursor> cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID);
        assertThat(cursor).isPresent();
        assertThat(cursor.get().hasRecordingId()).isFalse();
        assertThat(cursor.get().position()).isEqualTo(100L);
    }

    @Test
    void advanceWithRecording_firstWrite_inserts() {
        boolean changed = cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 1000L);
        assertThat(changed).isTrue();

        Optional<AeronProjectorCursorRepository.RecordedCursor> cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID);
        assertThat(cursor).isPresent();
        assertThat(cursor.get().hasRecordingId()).isTrue();
        assertThat(cursor.get().recordingId()).isEqualTo(13L);
        assertThat(cursor.get().position()).isEqualTo(1000L);
    }

    @Test
    void advanceWithRecording_sameRecording_advancesForward_noopBackwards() {
        cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 1000L);

        assertThat(cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 2000L)).isTrue();
        assertThat(cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get().position()).isEqualTo(2000L);

        assertThat(cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 1500L)).isFalse();
        assertThat(cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get().position()).isEqualTo(2000L);

        assertThat(cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 2000L)).isFalse();
        assertThat(cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get().position()).isEqualTo(2000L);
    }

    @Test
    void advanceWithRecording_newerRecording_winsEvenWithSmallerPosition() {
        // Cluster restart promotes the cursor to a new recording at position 0 — this MUST be
        // allowed even though position(0) < saved position(2000). Without this, the projector
        // could never roll forward to a freshly opened recording after a cluster restart.
        cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 2000L);

        boolean rolledForward = cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 16L, 0L);
        assertThat(rolledForward).isTrue();

        AeronProjectorCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(0L);
    }

    @Test
    void advanceWithRecording_olderRecording_rejected_evenWithLargerPosition() {
        // Aeron Archive recording ids never decrease. A write with a smaller recording id is a
        // wiring bug (projector pointed at the wrong Archive) or a corruption — must NOT silently
        // overwrite a saved newer-recording cursor.
        cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 16L, 500L);

        boolean rejected = cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 999_999L);
        assertThat(rejected).isFalse();

        AeronProjectorCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(500L);
    }

    @Test
    void advanceWithRecording_legacyNullRecordingRow_doesNotOverwrite() {
        // The pre-V55 row has last_applied_recording_id NULL. A recording-aware advance must
        // refuse to overwrite it silently — the projector's init() path requires operator
        // intervention to repair via resetWithRecording first.
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);

        boolean blocked = cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 999L);
        assertThat(blocked).isFalse();

        AeronProjectorCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get();
        assertThat(cursor.hasRecordingId()).isFalse();
        assertThat(cursor.position()).isEqualTo(100L);
    }

    @Test
    void resetWithRecording_overwritesUnconditionally_includingBackwards() {
        // Operator-driven repair must be able to land any (recordingId, position) — including
        // moving the cursor backwards if the operator has determined the saved state was wrong.
        cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 16L, 5_000L);

        cursorRepository.resetWithRecording(PROJECTOR_ID, STREAM_ID, 13L, 100L);

        AeronProjectorCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get();
        assertThat(cursor.recordingId()).isEqualTo(13L);
        assertThat(cursor.position()).isEqualTo(100L);
    }

    @Test
    void resetWithRecording_repairsLegacyNullRow() {
        // The pop 2026-05-23 recovery flow: operator runs resetWithRecording to populate the
        // missing recording id on a legacy row, then the projector can start.
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 42_464L);

        cursorRepository.resetWithRecording(PROJECTOR_ID, STREAM_ID, 16L, 0L);

        AeronProjectorCursorRepository.RecordedCursor cursor =
                cursorRepository.findLastAppliedCursor(PROJECTOR_ID, STREAM_ID).get();
        assertThat(cursor.hasRecordingId()).isTrue();
        assertThat(cursor.recordingId()).isEqualTo(16L);
        assertThat(cursor.position()).isEqualTo(0L);

        // After the repair, a recording-aware advance lands cleanly.
        assertThat(cursorRepository.advanceWithRecording(PROJECTOR_ID, STREAM_ID, 16L, 100L)).isTrue();
    }

    @Test
    void findLastAppliedAt_advancesOnUpsert_butNotOnNoopOlderWrite() throws InterruptedException {
        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 200L);
        Optional<Instant> firstTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(firstTs).isPresent();

        Thread.sleep(50);

        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 100L);
        Optional<Instant> staleTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(staleTs).contains(firstTs.get());

        Thread.sleep(50);

        cursorRepository.advance(PROJECTOR_ID, STREAM_ID, 300L);
        Optional<Instant> bumpedTs = cursorRepository.findLastAppliedAt(PROJECTOR_ID, STREAM_ID);
        assertThat(bumpedTs).isPresent();
        assertThat(bumpedTs.get()).isAfter(firstTs.get());
    }
}
