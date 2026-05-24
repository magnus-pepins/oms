package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for {@value OmsProfiles#FIX_INGRESS} (FIX-in acceptor role).
 *
 * <p>Mutually exclusive with every other worker profile. Requires cluster client for
 * {@code AcceptOrderCommand} admission and rejects gRPC order ingress on this JVM.
 */
@Component
@Profile("!test")
public class FixIngressTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public FixIngressTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validateFixIngressTopology(environment, omsConfig);
    }

    public static void validateFixIngressTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.FIX_INGRESS))) {
            return;
        }
        if (omsConfig.getGrpc().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_INGRESS
                            + " requires oms.grpc.enabled=false (no HTTP/gRPC order ingress on this JVM).");
        }
        if (!omsConfig.getCluster().getClient().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_INGRESS
                            + " requires oms.cluster.client.enabled=true (AcceptOrderCommand admission).");
        }
    }
}
