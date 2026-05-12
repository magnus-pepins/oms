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

    @Test
    void fixEgress_andFixWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.FIX_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void fixEgress_andControlWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.CONTROL_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.CONTROL_WORKER);
    }

    @Test
    void fixEgress_andIngressReplica_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.INGRESS_REPLICA);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA);
    }

    @Test
    void fixEgress_andPostgresProjector_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.POSTGRES_PROJECTOR);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.POSTGRES_PROJECTOR);
    }

    @Test
    void fixEgress_only_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        assertThatCode(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .doesNotThrowAnyException();
    }
}
