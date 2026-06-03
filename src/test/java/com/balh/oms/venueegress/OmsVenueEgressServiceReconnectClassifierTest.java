package com.balh.oms.venueegress;

import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OmsVenueEgressService#isRecoverableClusterInfraError(Throwable)}.
 *
 * <p>Regression guard for the 2026-06-03 pop incident: a transient OMS cluster MediaDriver bounce
 * threw {@code io.aeron.exceptions.TimeoutException} out of {@code listRecordingsForUri}, the
 * replay loop's terminal catch logged "replay loop terminating" and the {@code oms-venue-egress}
 * thread died permanently — silently severing the OMS->venue bridge so admitted orders never
 * routed to the venue and never left {@code PENDING_NEW}. The loop now distinguishes recoverable
 * infra timeouts (reconnect + resume) from fatal cursor/recording-state corruption (operator
 * intervention). These cases pin that distinction so a future refactor cannot regress it back to
 * "any RuntimeException kills the bridge".
 */
class OmsVenueEgressServiceReconnectClassifierTest {

    @Test
    void archiveTimeout_isRecoverable() {
        // The exact 17:25 failure: AeronArchive.listRecordingsForUri awaiting recording descriptors.
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new TimeoutException("awaiting recording descriptors"))).isTrue();
    }

    @Test
    void driverTimeout_isRecoverable() {
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new DriverTimeoutException("MediaDriver keepalive timeout"))).isTrue();
    }

    @Test
    void agentTermination_isRecoverable() {
        // Raised by the Aeron client conductor when the driver heartbeat is lost.
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new AgentTerminationException("driver timeout"))).isTrue();
    }

    @Test
    void registrationException_isRecoverable() {
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new RegistrationException(0L, 0, io.aeron.ErrorCode.GENERIC_ERROR, "channel error"))).isTrue();
    }

    @Test
    void wrappedTimeout_inCauseChain_isRecoverable() {
        RuntimeException wrapped = new RuntimeException(
                "replay open failed", new TimeoutException("awaiting recording descriptors"));
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(wrapped)).isTrue();
    }

    @Test
    void cursorStateCorruption_isNotRecoverable() {
        // What requireRecordingPresent throws when the saved recording id is gone from the Archive.
        // Must stay fatal: reconnecting would re-throw, and silently resetting risks data loss.
        IllegalStateException stateError = new IllegalStateException(
                "oms-venue-egress cannot continue: saved recordingId=387 is not listed in the Aeron Archive");
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(stateError)).isFalse();
    }

    @Test
    void genericRuntimeError_isNotRecoverable() {
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new RuntimeException("boom"))).isFalse();
    }

    @Test
    void nullCause_terminatesWalk() {
        assertThat(OmsVenueEgressService.isRecoverableClusterInfraError(
                new RuntimeException("no cause"))).isFalse();
    }
}
