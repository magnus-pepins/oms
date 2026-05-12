package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for {@value OmsProfiles#INGRESS_REPLICA} (horizontal ingress: order accept + ingress admission +
 * Chronicle append only; no local control tail).
 */
@Component
@Profile("!test")
public class IngressReplicaTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public IngressReplicaTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validateIngressReplicaTopology(environment, omsConfig);
    }

    public static void validateIngressReplicaTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.INGRESS_REPLICA))) {
            return;
        }
        if (!omsConfig.getChronicle().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.INGRESS_REPLICA
                            + " requires oms.chronicle.enabled=true (this JVM must append to the control Chronicle).");
        }
        if (omsConfig.getChronicle().isControlTailEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.INGRESS_REPLICA
                            + " requires oms.chronicle.control-tail-enabled=false "
                            + "(use control / FIX worker JVMs to consume the Chronicle tail).");
        }
        if (omsConfig.getControl().getPostgresWritePath() != ControlPostgresWritePath.INGRESS) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.INGRESS_REPLICA
                            + " requires oms.control.postgres-write-path=ingress "
                            + "(admission must run in the ingress transaction; see application-oms-ingress-replica.yaml).");
        }
        if ("fix".equalsIgnoreCase(omsConfig.getRouting().getBackend()) && omsConfig.getFix().isAutoStart()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.INGRESS_REPLICA
                            + " cannot run QuickFIX with oms.routing.backend=fix and oms.fix.auto-start=true "
                            + "(single SocketInitiator per route); use Spring profile "
                            + OmsProfiles.FIX_WORKER
                            + " for the FIX-out JVM.");
        }
    }
}
