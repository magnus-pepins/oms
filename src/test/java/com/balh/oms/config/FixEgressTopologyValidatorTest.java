package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixEgressTopologyValidatorTest {

    @Test
    void fixEgressProfile_grpcOff_clusterClientOff_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(false);
        assertThatCode(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void fixEgressProfile_grpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getCluster().getClient().setEnabled(false);
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void fixEgressProfile_clusterClientEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.cluster.client.enabled=false");
    }

    @Test
    void fixEgressWithFixWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void fixEgressWithControlWorker_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.CONTROL_WORKER);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.CONTROL_WORKER);
    }

    @Test
    void fixEgressWithIngressReplica_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA);
    }

    @Test
    void fixEgressWithPostgresProjector_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS, OmsProfiles.POSTGRES_PROJECTOR);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS)
                .hasMessageContaining(OmsProfiles.POSTGRES_PROJECTOR);
    }

    @Test
    void withoutFixEgressProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("some-other-profile");
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatCode(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .doesNotThrowAnyException();
    }
}
