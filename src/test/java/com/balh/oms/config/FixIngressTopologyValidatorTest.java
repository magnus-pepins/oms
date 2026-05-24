package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixIngressTopologyValidatorTest {

    @Test
    void fixIngress_withClusterClient_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_INGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(true);
        assertThatCode(() -> FixIngressTopologyValidator.validateFixIngressTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void fixIngress_withoutClusterClient_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_INGRESS);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getCluster().getClient().setEnabled(false);
        assertThatThrownBy(() -> FixIngressTopologyValidator.validateFixIngressTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.cluster.client.enabled=true");
    }
}
