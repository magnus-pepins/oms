package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixEgressTopologyValidatorTest {

    @Test
    void fixEgressProfile_grpcOff_clusterClientOn_ok() {
        // Slice 3d invariant: cluster.client.enabled MUST be true on oms-fix-egress so the inbound
        // FixInboundClusterSink can offer ApplyExecutionReportCommand back to the cluster.
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatCode(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void fixEgressProfile_grpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void fixEgressProfile_clusterClientDisabled_throws() {
        // Slice 3d: validator now REQUIRES cluster.client.enabled=true on oms-fix-egress (was
        // rejected in slices 3a/3b). The flip mirrors the behaviour change in the role: egress now
        // both reads from the events recording AND writes back via OmsClusterIngressClient on
        // inbound venue ER.
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(false);
        assertThatThrownBy(() -> FixEgressTopologyValidator.validateFixEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.cluster.client.enabled=true");
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
