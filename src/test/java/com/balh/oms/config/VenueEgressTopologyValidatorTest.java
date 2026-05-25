package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VenueEgressTopologyValidatorTest {

    @Test
    void venueEgressProfile_grpcOff_clusterClientOn_ok() {
        // Slice 3d invariant: cluster.client.enabled MUST be true on oms-fix-egress so the inbound
        // FixInboundClusterSink can offer ApplyExecutionReportCommand back to the cluster.
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.VENUE_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getRouting().setBackend("internal-venue");
        assertThatCode(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void venueEgressProfile_grpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.VENUE_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatThrownBy(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void venueEgressProfile_clusterClientDisabled_throws() {
        // Slice 3d: validator now REQUIRES cluster.client.enabled=true on oms-fix-egress (was
        // rejected in slices 3a/3b). The flip mirrors the behaviour change in the role: egress now
        // both reads from the events recording AND writes back via OmsClusterIngressClient on
        // inbound venue ER.
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.VENUE_EGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(false);
        assertThatThrownBy(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.cluster.client.enabled=true");
    }

    @Test
    void venueEgressWithIngressReplica_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.VENUE_EGRESS, OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.VENUE_EGRESS)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA);
    }

    @Test
    void venueEgressWithPostgresProjector_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.VENUE_EGRESS, OmsProfiles.POSTGRES_PROJECTOR);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.VENUE_EGRESS)
                .hasMessageContaining(OmsProfiles.POSTGRES_PROJECTOR);
    }

    @Test
    void withoutVenueEgressProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("some-other-profile");
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getRouting().setBackend("internal-venue");
        assertThatCode(() -> VenueEgressTopologyValidator.validateVenueEgressTopology(env, cfg))
                .doesNotThrowAnyException();
    }
}
