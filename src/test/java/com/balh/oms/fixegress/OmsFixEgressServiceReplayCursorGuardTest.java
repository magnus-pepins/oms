package com.balh.oms.fixegress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 2026-05-23 hardening — egress twin of
 * {@link com.balh.oms.projector.OmsPostgresProjectorReplayCursorGuardTest}. Pins the egress'
 * recording-presence and position-bounds guards so the loud-fail messages stay
 * operator-actionable across future refactors.
 *
 * <p>Closes gap 5 of the post-V55/V56 stability review: the legacy-NULL branch is exercised
 * by {@link OmsFixEgressServiceInitLoudFailTest}; this test covers the two replay-loop
 * predicates that previously only ran against a real {@link io.aeron.archive.client.AeronArchive}.
 */
class OmsFixEgressServiceReplayCursorGuardTest {

    @Test
    void requireRecordingPresent_whenDescriptorPresent_returnsNormally() {
        var descriptor = new OmsFixEgressService.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsFixEgressService.requireRecordingPresent(16L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requireRecordingPresent_whenDescriptorAbsent_failsLoudWithRepairContext() {
        assertThatThrownBy(() -> OmsFixEgressService.requireRecordingPresent(16L, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms-fix-egress cannot continue")
                .hasMessageContaining("saved recordingId=16")
                .hasMessageContaining("is not listed in the Aeron Archive")
                .hasMessageContaining("events stream " + OmsClusterWireFormat.EVENTS_STREAM_ID)
                .hasMessageContaining("channel=" + OmsClusterWireFormat.EVENTS_CHANNEL)
                .hasMessageContaining("resetWithRecording")
                .hasMessageContaining("handover §9.6");
    }

    @Test
    void requirePositionWithinRecording_atUpperBound_returnsNormally() {
        var descriptor = new OmsFixEgressService.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsFixEgressService.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 4096L, /* upperBound = */ 4096L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requirePositionWithinRecording_belowUpperBound_returnsNormally() {
        var descriptor = new OmsFixEgressService.RecordingDescriptor(16L, 0L, 4096L);

        assertThatCode(() -> OmsFixEgressService.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 100L, /* upperBound = */ 4096L, descriptor))
                .doesNotThrowAnyException();
    }

    @Test
    void requirePositionWithinRecording_pastUpperBound_failsLoudWithRecordingBounds() {
        var descriptor = new OmsFixEgressService.RecordingDescriptor(
                /* recordingId = */ 16L,
                /* startPosition = */ 0L,
                /* stopPosition = */ 4096L);

        assertThatThrownBy(() -> OmsFixEgressService.requirePositionWithinRecording(
                        16L, /* savedPosition = */ 8192L, /* upperBound = */ 4096L, descriptor))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms-fix-egress saved cursor")
                .hasMessageContaining("recordingId=16")
                .hasMessageContaining("position=8192")
                .hasMessageContaining("upperBound=4096")
                .hasMessageContaining("startPosition=0")
                .hasMessageContaining("stopPosition=4096")
                .hasMessageContaining("Refusing to silently reset to position 0");
    }
}
