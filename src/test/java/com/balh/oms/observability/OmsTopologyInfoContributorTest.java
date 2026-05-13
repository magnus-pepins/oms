package com.balh.oms.observability;

import com.balh.oms.config.OmsConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.info.Info;
import org.springframework.mock.env.MockEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OmsTopologyInfoContributorTest {

    @Test
    void contributesEffectiveTopologyMap() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRouting().setBackend("fix");
        cfg.getGrpc().setEnabled(false);
        cfg.getFix().setAutoStart(true);

        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles("beta", "alpha");
        OmsTopologyInfoContributor contributor = new OmsTopologyInfoContributor(cfg, env);
        Info.Builder builder = new Info.Builder();
        contributor.contribute(builder);
        Info info = builder.build();

        @SuppressWarnings("unchecked")
        Map<String, Object> topology = (Map<String, Object>) info.getDetails().get("oms-topology");
        assertThat(topology).isNotNull();
        assertThat(topology.get("routing.backend")).isEqualTo("fix");
        assertThat(topology.get("grpc.enabled")).isEqualTo(false);
        assertThat(topology.get("fix.auto-start")).isEqualTo(true);
        assertThat(topology.get("spring.active-profiles")).isEqualTo("alpha,beta");
    }
}
