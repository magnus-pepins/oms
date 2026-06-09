package com.balh.oms.cluster;

import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OmsClusterEventsRecordingSupport#isRecoverableClusterInfraError(Throwable)}. */
class OmsClusterEventsRecordingSupportReconnectTest {

    @Test
    void timeoutException_isRecoverable() {
        assertThat(OmsClusterEventsRecordingSupport.isRecoverableClusterInfraError(
                        new TimeoutException("archive timeout")))
                .isTrue();
    }

    @Test
    void driverTimeoutException_isRecoverable() {
        assertThat(OmsClusterEventsRecordingSupport.isRecoverableClusterInfraError(
                        new DriverTimeoutException("driver timeout")))
                .isTrue();
    }

    @Test
    void agentTerminationException_isRecoverable() {
        assertThat(OmsClusterEventsRecordingSupport.isRecoverableClusterInfraError(
                        new AgentTerminationException("agent terminated")))
                .isTrue();
    }

    @Test
    void wrappedTimeout_isRecoverable() {
        RuntimeException wrapped = new RuntimeException("outer", new TimeoutException("inner"));
        assertThat(OmsClusterEventsRecordingSupport.isRecoverableClusterInfraError(wrapped)).isTrue();
    }

    @Test
    void illegalState_isNotRecoverable() {
        assertThat(OmsClusterEventsRecordingSupport.isRecoverableClusterInfraError(
                        new IllegalStateException("cursor corruption")))
                .isFalse();
    }
}
