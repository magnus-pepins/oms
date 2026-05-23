package com.balh.oms.projector;

import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OmsPostgresProjector#decideWalkForward(long, long, OmsPostgresProjector.RecordingDescriptor)}.
 *
 * <p>This is the pure decision predicate behind the per-poll roll-forward logic inside
 * {@code runReplayLoopWithRecordingWalk}. It's extracted to a static helper so the four
 * meaningfully different situations can be asserted without spinning up an Aeron Archive:
 *
 * <ol>
 *   <li>No successor exists at all → park (live tail on the cluster's currently-active
 *       recording).</li>
 *   <li>Current recording is closed AND we've fully drained it AND a successor exists →
 *       walk forward (the historic "boundary" case).</li>
 *   <li>Current recording is closed but we haven't drained it yet → park (events still to
 *       read from the closed recording).</li>
 *   <li>Current recording is stuck-open ({@code stopPosition = NULL_POSITION}) AND a
 *       successor exists → walk forward (the post-crash case that hit pop on 2026-05-23
 *       and left the {@code cluster=9 projector=0} drift unresolved).</li>
 * </ol>
 */
class OmsPostgresProjectorWalkForwardTest {

    private static OmsPostgresProjector.RecordingDescriptor successor(long recordingId, long startPosition) {
        return new OmsPostgresProjector.RecordingDescriptor(recordingId, startPosition, AeronArchive.NULL_POSITION);
    }

    @Test
    void noSuccessor_parks() {
        OmsPostgresProjector.WalkForwardDecision decision =
                OmsPostgresProjector.decideWalkForward(
                        /* recordingStop = */ 1_000L,
                        /* currentApplied = */ 1_000L,
                        /* successor = */ null);

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }

    @Test
    void closedAndDrained_withSuccessor_walks() {
        // Current recording is closed (stop=1000) and we've reached its tail (applied=1000).
        // A successor exists. Walk forward — the historic "recording boundary" case the V55
        // code already handled correctly.
        OmsPostgresProjector.WalkForwardDecision decision =
                OmsPostgresProjector.decideWalkForward(
                        /* recordingStop = */ 1_000L,
                        /* currentApplied = */ 1_000L,
                        successor(17L, 0L));

        assertThat(decision.walk()).isTrue();
        assertThat(decision.reason()).isEqualTo("complete");
    }

    @Test
    void closedButNotDrained_withSuccessor_parks() {
        // Closed recording with 500 bytes remaining to read. Successor exists but we still
        // have work on the current recording — keep polling, don't walk yet.
        OmsPostgresProjector.WalkForwardDecision decision =
                OmsPostgresProjector.decideWalkForward(
                        /* recordingStop = */ 1_000L,
                        /* currentApplied = */ 500L,
                        successor(17L, 0L));

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }

    @Test
    void staleOpen_withSuccessor_walks() {
        // stopPosition = NULL_POSITION (never closed) + successor exists. This is the
        // post-crash case: the cluster session that wrote the current recording died
        // without flushing the stop position, then a new cluster session created a new
        // (higher-id) recording. Without walking forward the projector would park forever
        // on the dead recording. Reason string must say "stale-open" so operators can grep
        // for this distinct failure mode (vs "complete").
        OmsPostgresProjector.WalkForwardDecision decision =
                OmsPostgresProjector.decideWalkForward(
                        /* recordingStop = */ AeronArchive.NULL_POSITION,
                        /* currentApplied = */ 0L,
                        successor(17L, 0L));

        assertThat(decision.walk()).isTrue();
        assertThat(decision.reason()).isEqualTo("stale-open (successor exists)");
    }

    @Test
    void staleOpen_noSuccessor_parks() {
        // stopPosition = NULL_POSITION + no successor → genuine live tail on the cluster's
        // currently-active recording (cluster just hasn't published any events yet, or
        // we've caught up). Park.
        OmsPostgresProjector.WalkForwardDecision decision =
                OmsPostgresProjector.decideWalkForward(
                        /* recordingStop = */ AeronArchive.NULL_POSITION,
                        /* currentApplied = */ 0L,
                        /* successor = */ null);

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }
}
