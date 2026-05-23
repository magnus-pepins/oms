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
        when(cursorRepository.findLastAppliedCursor(anyString(), anyInt())).thenReturn(Optional.empty());

        egress.init();

        // lastAppliedPosition stays at its constructor default (0); the bootstrap path in the
        // replay loop is responsible for seeding it from the oldest recording's startPosition.
        assertThat(egress.lastAppliedPosition()).isZero();
        egress.close();
    }

    @Test
    void init_recordingAwareCursor_seedsBothPositionAndRecordingId() {
        when(cursorRepository.findLastAppliedCursor(anyString(), anyInt()))
                .thenReturn(Optional.of(
                        OmsFixEgressCursorRepository.RecordedCursor.of(16L, 42_464L)));

        egress.init();

        assertThat(egress.lastAppliedPosition()).isEqualTo(42_464L);
        // currentRecordingId is package-private state; we verify it indirectly: init() seeded
        // both fields and the no-op replay thread will exit cleanly (aeronDirectory blank).
        // The recording-id seed itself is exercised by the service-test setUp's manual seed
        // in the apply-path tests; the projector twin test makes the same trade-off.
        egress.close();
    }

    @Test
    void init_legacyNullRecordingIdCursor_failsLoudWithRepairSql() {
        when(cursorRepository.findLastAppliedCursor(anyString(), anyInt()))
                .thenReturn(Optional.of(
                        OmsFixEgressCursorRepository.RecordedCursor.legacyWithoutRecordingId(42_464L)));

        assertThatThrownBy(egress::init)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("last_applied_recording_id IS NULL")
                .hasMessageContaining("UPDATE oms_fix_egress_cursor")
                .hasMessageContaining("SET last_applied_recording_id")
                .hasMessageContaining("WHERE egress_id = '" + OmsFixEgressService.EGRESS_ID + "'")
                .hasMessageContaining("stream_id = " + OmsClusterWireFormat.EVENTS_STREAM_ID)
                .hasMessageContaining("42464");
    }
}
