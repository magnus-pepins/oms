package com.balh.oms.projector;

import com.balh.oms.cluster.OmsClusterWireFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 2026-05-23 hardening — pins the projector's two replay-loop loud-fail predicates so we can
 * exercise them without standing up a full Aeron Archive. The original code path is reached
 * only when a real {@link io.aeron.archive.client.AeronArchive} returns either an empty
 * {@code listRecordingsForUri} (recording missing) or a {@code getRecordingPosition} below the
 * cursor (position past end); recreating that environment in JUnit is not worth the cost.
 *
 * <p>Closes gap 5 of the post-V55/V56 stability review: previously
 * {@code requireRecordingPresent} and {@code requirePositionWithinRecording} lived inline in
 * {@code runReplayLoopWithRecordingWalk} with no unit coverage. The on-pop deploy proved the
 * legacy-NULL branch fires; this test proves the other two branches throw the right message
 * with the right operator-actionable diagnostics.
 */
class OmsPostgresProjectorReplayCursorGuardTest {

    @Test
    void requireRecordingPresent_whenDescriptorPresent_returnsNormally() {
        var descriptor = new OmsPostgresProjector.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsPostgresProjector.requireRecordingPresent(16L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requireRecordingPresent_whenDescriptorAbsent_failsLoudWithRepairContext() {
        assertThatThrownBy(() -> OmsPostgresProjector.requireRecordingPresent(16L, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms-postgres-projector cannot continue")
                .hasMessageContaining("saved recordingId=16")
                .hasMessageContaining("is not listed in the Aeron Archive")
                .hasMessageContaining("events stream " + OmsClusterWireFormat.EVENTS_STREAM_ID)
                .hasMessageContaining("channel=" + OmsClusterWireFormat.EVENTS_CHANNEL)
                .hasMessageContaining("resetWithRecording")
                .hasMessageContaining("handover §9");
    }

    @Test
    void requirePositionWithinRecording_atUpperBound_returnsNormally() {
        var descriptor = new OmsPostgresProjector.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsPostgresProjector.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 4096L, /* upperBound = */ 4096L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requirePositionWithinRecording_belowUpperBound_returnsNormally() {
        var descriptor = new OmsPostgresProjector.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsPostgresProjector.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 100L, /* upperBound = */ 4096L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requirePositionWithinRecording_pastUpperBound_failsLoudWithRecordingBounds() {
        var descriptor = new OmsPostgresProjector.RecordingDescriptor(
                /* recordingId = */ 16L,
                /* startPosition = */ 0L,
                /* stopPosition = */ 4096L);

        assertThatThrownBy(() -> OmsPostgresProjector.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 8192L, /* upperBound = */ 4096L, descriptor))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms-postgres-projector saved cursor")
                .hasMessageContaining("recordingId=16")
                .hasMessageContaining("position=8192")
                .hasMessageContaining("upperBound=4096")
                .hasMessageContaining("startPosition=0")
                .hasMessageContaining("stopPosition=4096")
                .hasMessageContaining("Refusing to silently reset to position 0");
    }
}
