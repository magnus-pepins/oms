package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyWorkerProfilesTest {

    @Test
    void controlWorker_andFixWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER, OmsProfiles.FIX_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.CONTROL_WORKER)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void ingressReplica_andControlWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA, OmsProfiles.CONTROL_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA)
                .hasMessageContaining(OmsProfiles.CONTROL_WORKER);
    }

    @Test
    void ingressReplica_andFixWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA, OmsProfiles.FIX_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void ingressReplica_only_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        assertThatCode(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .doesNotThrowAnyException();
    }

    @Test
    void fixWorker_only_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        assertThatCode(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .doesNotThrowAnyException();
    }
}
