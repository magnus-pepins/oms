package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixWorkerTopologyValidatorTest {

    @Test
    void fixWorker_withGrpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void fixWorker_backendNotFix_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("noop");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.routing.backend=fix");
    }

    @Test
    void fixWorker_autoStartFalse_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(false);
        assertThatThrownBy(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.fix.auto-start=true");
    }

    @Test
    void fixWorker_grpcOffFixAuto_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatCode(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg)).doesNotThrowAnyException();
    }

    @Test
    void fixWorker_postgresWritePathTail_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getControl().setPostgresWritePath("tail");
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("postgres-write-path=ingress");
    }

    @Test
    void fixWorker_controlTailDisabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.FIX_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        cfg.getControl().setPostgresWritePath("ingress");
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        assertThatThrownBy(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("control-tail-enabled=true");
    }

    @Test
    void bothWorkerProfiles_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER, OmsProfiles.FIX_WORKER);
        assertThatThrownBy(() -> TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot activate");
    }

    @Test
    void withoutFixWorkerProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("some-other-profile");
        OmsConfig cfg = new OmsConfig();
        assertThatCode(() -> FixWorkerTopologyValidator.validateFixWorkerTopology(env, cfg)).doesNotThrowAnyException();
    }
}
