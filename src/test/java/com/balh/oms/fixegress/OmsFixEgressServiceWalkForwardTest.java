package com.balh.oms.fixegress;

import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OmsFixEgressService#decideWalkForward(long, long, OmsFixEgressService.RecordingDescriptor)}.
 *
 * <p>Mirrors {@code OmsPostgresProjectorWalkForwardTest} — the egress and the projector run
 * the same recording-walk machinery against the same Aeron Archive, so both must make the
 * same walk-forward decisions on the same inputs. Drift between the two would mean one
 * service keeps up with cluster restarts while the other stalls, which is the failure mode
 * the 2026-05-23 hardening was meant to eliminate.
 */
class OmsFixEgressServiceWalkForwardTest {

    private static OmsFixEgressService.RecordingDescriptor successor(long recordingId, long startPosition) {
        return new OmsFixEgressService.RecordingDescriptor(recordingId, startPosition, AeronArchive.NULL_POSITION);
    }

    @Test
    void noSuccessor_parks() {
        OmsFixEgressService.WalkForwardDecision decision =
                OmsFixEgressService.decideWalkForward(1_000L, 1_000L, null);

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }

    @Test
    void closedAndDrained_withSuccessor_walks() {
        OmsFixEgressService.WalkForwardDecision decision =
                OmsFixEgressService.decideWalkForward(1_000L, 1_000L, successor(17L, 0L));

        assertThat(decision.walk()).isTrue();
        assertThat(decision.reason()).isEqualTo("complete");
    }

    @Test
    void closedButNotDrained_withSuccessor_parks() {
        OmsFixEgressService.WalkForwardDecision decision =
                OmsFixEgressService.decideWalkForward(1_000L, 500L, successor(17L, 0L));

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }

    @Test
    void staleOpen_withSuccessor_walks() {
        // The 2026-05-23 pop case: dead recording 16 stopPosition=-1 (cluster crashed
        // without flushing), live recording 17 exists. Walk forward — without this branch
        // the egress would never emit FIX for new admits, dropping orders silently.
        OmsFixEgressService.WalkForwardDecision decision =
                OmsFixEgressService.decideWalkForward(
                        AeronArchive.NULL_POSITION, 0L, successor(17L, 0L));

        assertThat(decision.walk()).isTrue();
        assertThat(decision.reason()).isEqualTo("stale-open (successor exists)");
    }

    @Test
    void staleOpen_noSuccessor_parks() {
        OmsFixEgressService.WalkForwardDecision decision =
                OmsFixEgressService.decideWalkForward(
                        AeronArchive.NULL_POSITION, 0L, null);

        assertThat(decision.walk()).isFalse();
        assertThat(decision.reason()).isEqualTo("park");
    }
}
