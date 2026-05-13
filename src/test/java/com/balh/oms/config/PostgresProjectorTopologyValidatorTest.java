package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresProjectorTopologyValidatorTest {

    @Test
    void projectorProfile_grpcOff_fixOff_ok() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.POSTGRES_PROJECTOR);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getFix().setAutoStart(false);
        assertThatCode(() -> PostgresProjectorTopologyValidator.validatePostgresProjectorTopology(env, cfg))
                .doesNotThrowAnyException();
    }

    @Test
    void projectorProfile_grpcEnabled_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.POSTGRES_PROJECTOR);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(true);
        assertThatThrownBy(() -> PostgresProjectorTopologyValidator.validatePostgresProjectorTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.grpc.enabled=false");
    }

    @Test
    void projectorProfile_fixBackendWithAutoStart_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.POSTGRES_PROJECTOR);
        OmsConfig cfg = new OmsConfig();
        cfg.getGrpc().setEnabled(false);
        cfg.getRouting().setBackend("fix");
        cfg.getFix().setAutoStart(true);
        assertThatThrownBy(() -> PostgresProjectorTopologyValidator.validatePostgresProjectorTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.FIX_EGRESS);
    }

    @Test
    void projectorWithIngressReplica_throws() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(OmsProfiles.POSTGRES_PROJECTOR, OmsProfiles.INGRESS_REPLICA);
        OmsConfig cfg = new OmsConfig();
        assertThatThrownBy(() -> PostgresProjectorTopologyValidator.validatePostgresProjectorTopology(env, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OmsProfiles.POSTGRES_PROJECTOR)
                .hasMessageContaining(OmsProfiles.INGRESS_REPLICA);
    }

    @Test
    void withoutProjectorProfile_noop() {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("some-other-profile");
        OmsConfig cfg = new OmsConfig();
        assertThatCode(() -> PostgresProjectorTopologyValidator.validatePostgresProjectorTopology(env, cfg))
                .doesNotThrowAnyException();
    }
}
