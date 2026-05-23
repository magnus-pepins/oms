package com.balh.oms.fixegress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 2026-05-23 hardening (V56) — pins the {@link OmsFixEgressService#init} loud-fail contract
 * that prevents the egress-side of the pop incident from recurring. Mirrors
 * {@code OmsPostgresProjectorInitLoudFailTest} one-for-one against the egress' cursor table.
 *
 * <p>Three startup cases must behave as described in the egress' class Javadoc + handover §9.6:
 *
 * <ol>
 *   <li><b>Empty cursor table</b> (first-ever egress start): {@code init()} must succeed and
 *       leave the replay loop free to bootstrap from the oldest recording.</li>
 *   <li><b>Recording-aware cursor</b> (post-V56 normal): {@code init()} must succeed and seed
 *       both {@code lastAppliedPosition} and {@code currentRecordingId} from the saved row.</li>
 *   <li><b>Legacy NULL cursor</b> (pre-V56 row, or a row corrupted by the pre-2026-05-23
 *       silent fallback): {@code init()} must <b>throw</b> with an operator-actionable message
 *       naming the repair SQL. The replay thread must not start; the JVM context must not come
 *       up.</li>
 * </ol>
 *
 * <p>Drives {@code init()} directly so the test runs without bringing up a Spring context, an
 * Aeron cluster, or QuickFIX. The replay loop's {@code aeronDirectory.isBlank()} guard means
 * the spawned thread exits with a warn log immediately after running.set(true), without
 * touching Aeron — race-free for this test.
 */
@ExtendWith(MockitoExtension.class)
class OmsFixEgressServiceInitLoudFailTest {

    @Mock private OmsFixEgressCursorRepository cursorRepository;

    private OmsConfig config;
    private OmsFixEgressService egress;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        // Replay loop checks aeronDirectory.isBlank() and short-circuits with warn-and-return.
        // init()-only tests get a no-op replay thread that exits before doing anything observable.
        config.getCluster().getFixEgress().setAeronDirectory("");
        egress = new OmsFixEgressService(
                config,
                cursorRepository,
                new SimpleMeterRegistry(),
                Clock.systemUTC(),
                /* newOrderSingleBuilder = */ null,
                /* orderCancelRequestBuilder = */ null,
                /* orderCancelReplaceRequestBuilder = */ null,
                /* fixOutboundSessionSend = */ null);
    }

    @Test
    void init_emptyCursorTable_succeedsAndDefersToBootstrap() {
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.empty());

        egress.init();

        // lastAppliedPosition stays at its constructor default (0); the bootstrap path in the
        // replay loop is responsible for seeding it from the oldest recording's startPosition.
        assertThat(egress.lastAppliedPosition()).isZero();
        egress.close();
    }

    @Test
    void init_recordingAwareCursor_seedsBothPositionAndRecordingId() {
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L),
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L))));

        egress.init();

        assertThat(egress.lastAppliedPosition()).isEqualTo(42_464L);
        egress.close();
    }

    @Test
    void init_legacyNullRecordingIdCursor_failsLoudWithRepairSql() {
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.legacyWithoutRecordingId(42_464L),
                        null)));

        assertThatThrownBy(egress::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("last_applied_recording_id IS NULL")
                .hasMessageContaining("UPDATE oms_fix_egress_cursor")
                .hasMessageContaining("SET last_applied_recording_id")
                .hasMessageContaining("WHERE egress_id = '" + OmsFixEgressService.EGRESS_ID + "'")
                .hasMessageContaining("stream_id = " + OmsClusterWireFormat.EVENTS_STREAM_ID)
                .hasMessageContaining("42464");
    }

    @Test
    void init_v60_preV60RowWithNullHighWater_succeedsBecauseHighWaterSeedsOnFirstAdvance() {
        // High-water columns NULL (row was last written by pre-V60 code or by V60
        // resetWithRecording on a fresh insert). init() treats this as a normal recording-aware
        // resume; first successful advance after start lockstep-bumps the high-water mark.
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L),
                        null)));

        assertThatCode(egress::init).doesNotThrowAnyException();
        assertThat(egress.lastAppliedPosition()).isEqualTo(42_464L);
        egress.close();
    }

    @Test
    void init_v60_lastAppliedEqualsHighWater_succeedsNoRewindDetected() {
        // Normal steady state: cursor advanced through advanceWithRecording so both pairs are
        // in sync. Lex-compare returns 0; isRewound() returns false.
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L),
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L))));

        assertThatCode(egress::init).doesNotThrowAnyException();
        egress.close();
    }

    @Test
    void init_v60_rewoundWithinSameRecording_failsLoudWithHighWaterAck() {
        // Operator UPDATEd last_applied_position backwards within the same recording. Egress
        // refuses with a SQL repair string that zeroes the high-water mark to (16, 100).
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 100L),
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L))));

        assertThatThrownBy(egress::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cursor has been rewound")
                .hasMessageContaining("last_applied=(recordingId=16, position=100)")
                .hasMessageContaining("high_water=(recordingId=16, position=42464)")
                .hasMessageContaining("re-ship admit events as duplicate NOS")
                .hasMessageContaining("high_water_recording_id = 16")
                .hasMessageContaining("high_water_position = 100")
                .hasMessageContaining("handover");
    }

    @Test
    void init_v60_rewoundToLowerRecording_failsLoudWithHighWaterAck() {
        // Operator rewound the cursor to a lower recording id (e.g. (12, 0) after pin to (16, 0)).
        // Egress refuses; repair SQL targets the lower (12, 0) pair.
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(12L, 0L),
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 0L))));

        assertThatThrownBy(egress::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cursor has been rewound")
                .hasMessageContaining("last_applied=(recordingId=12, position=0)")
                .hasMessageContaining("high_water=(recordingId=16, position=0)")
                .hasMessageContaining("high_water_recording_id = 12")
                .hasMessageContaining("high_water_position = 0");
    }

    @Test
    void init_v60_rewindAckBySettingHighWaterToLastApplied_allowsStart() {
        // Operator did the rewind AND zeroed high_water (= last_applied). isRewound() returns
        // false because compareLex == 0. Start permitted.
        when(cursorRepository.findLastAppliedCursorWithHighWater(anyString(), anyInt()))
                .thenReturn(Optional.of(new OmsFixEgressCursorRepository.RecordedCursorWithHighWater(
                        OmsFixEgressCursorRepository.RecordedCursor.of(12L, 0L),
                        OmsFixEgressCursorRepository.RecordedCursor.of(12L, 0L))));

        assertThatCode(egress::init).doesNotThrowAnyException();
        egress.close();
    }
}
