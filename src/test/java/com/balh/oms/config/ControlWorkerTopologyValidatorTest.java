package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ControlWorkerTopologyValidatorTest {

    @Test
    void controlWorkerProfile_withGrpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        assertThatThrownBy(() -> ControlWorkerTopologyValidator.validateTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void controlWorkerProfile_grpcOff_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        assertThatCode(() -> ControlWorkerTopologyValidator.validateTopology(env, cfg)).doesNotThrowAnyException();
    }

    @Test
    void controlWorkerProfile_fixBackendWithAutoStart_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> ControlWorkerTopologyValidator.validateTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_WORKER);
    }

    @Test
    void controlWorkerProfile_controlTailDisabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.CONTROL_WORKER);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        assertThatThrownBy(() -> ControlWorkerTopologyValidator.validateTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("control-tail-enabled=true");
    }

    @Test
    void withoutControlWorkerProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("some-other-profile");
        OmsConfig cfg = new OmsConfig();
        assertThatCode(() -> ControlWorkerTopologyValidator.validateTopology(env, cfg)).doesNotThrowAnyException();
    }
}
