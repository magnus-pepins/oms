package com.balh.oms.observability;

import com.balh.oms.config.OmsConfig;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exposes effective split-topology knobs on {@code GET /actuator/info} under {@code oms-topology} so operators can
 * prove what this JVM is running (no guessing from YAML merges).
 */
@Component
public class OmsTopologyInfoContributor implements InfoContributor {

    private final OmsConfig omsConfig;
    private final Environment environment;

    public OmsTopologyInfoContributor(OmsConfig omsConfig, Environment environment) {
        this.omsConfig = omsConfig;
        this.environment = environment;
    }

    @Override
    public void contribute(Info.Builder builder) {
        Map<String, Object> topology = new LinkedHashMap<>();
        topology.put(
                "control.postgres-write-path",
                omsConfig.getControl().getPostgresWritePath().name().toLowerCase());
        topology.put("chronicle.enabled", omsConfig.getChronicle().isEnabled());
        topology.put("chronicle.control-tail-enabled", omsConfig.getChronicle().isControlTailEnabled());
        topology.put("routing.backend", omsConfig.getRouting().getBackend());
        topology.put("grpc.enabled", omsConfig.getGrpc().isEnabled());
        topology.put("fix.auto-start", omsConfig.getFix().isAutoStart());
        String[] profiles = environment.getActiveProfiles();
        Arrays.sort(profiles);
        topology.put("spring.active-profiles", String.join(",", profiles));
        builder.withDetail("oms-topology", topology);
    }
}
