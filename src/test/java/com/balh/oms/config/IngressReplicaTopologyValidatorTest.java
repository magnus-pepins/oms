package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngressReplicaTopologyValidatorTest {

    @Test
    void withoutIngressReplicaProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("other");
        OmsConfig cfg = new OmsConfig();
        assertThatCode(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void ingressReplica_chronicleDisabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(false);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("ingress");
        assertThatThrownBy(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.chronicle.enabled=true");
    }

    @Test
    void ingressReplica_controlTailEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(true);
        cfg.getControl().setPostgresWritePath("ingress");
        assertThatThrownBy(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("control-tail-enabled=false");
    }

    @Test
    void ingressReplica_postgresWritePathTail_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("tail");
        assertThatThrownBy(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("postgres-write-path=ingress");
    }

    @Test
    void ingressReplica_fixBackendAutoStart_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void ingressReplica_valid_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getControl().setChronicleAppendMode("ingress-after-commit");
        cfg.getRouting().setBackend("noop");
        assertThatCode(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void ingressReplica_reconcilerAppendMode_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getControl().setChronicleAppendMode("reconciler");
        assertThatThrownBy(() -> IngressReplicaTopologyValidator.validateIngressReplicaTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("chronicle-append-mode")
                .hasMessageContaining("ingress-after-commit");
    }
}
