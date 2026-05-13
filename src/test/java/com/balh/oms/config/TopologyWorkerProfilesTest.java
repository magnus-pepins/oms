package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyWorkerProfilesTest {

    @Test
    void ingressReplica_only_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        assertThatCode(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .doesNotThrowAnyException();
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
    void postgresProjector_andIngressReplica_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.POSTGRES_PROJECTOR, OmsProfiles.INGRESS_REPLICA);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.POSTGRES_PROJECTOR)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA);
    }

    @Test
    void fixEgress_only_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        assertThatCode(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .doesNotThrowAnyException();
    }
}
