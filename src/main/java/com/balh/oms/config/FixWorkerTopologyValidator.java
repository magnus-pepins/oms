package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for {@value OmsProfiles#FIX_WORKER} (FIX-out JVM: QuickFIX initiator allowed; no order-accept).
 *
 * <p>Excluded from the {@code test} profile so integration tests can use {@value OmsProfiles#FIX_WORKER} with
 * {@code oms.fix.auto-start=false} without binding a real broker socket.
 */
@Component
@Profile("!test")
public class FixWorkerTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public FixWorkerTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validateFixWorkerTopology(environment, omsConfig);
    }

    public static void validateFixWorkerTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.FIX_WORKER))) {
            return;
        }
        if (omsConfig.getGrpc().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_WORKER
                            + " requires oms.grpc.enabled=false (no OrderIngress on this JVM).");
        }
        if (!"fix".equalsIgnoreCase(omsConfig.getRouting().getBackend())) {
            throw new IllegalStateException(
                    "Spring profile " + OmsProfiles.FIX_WORKER + " requires oms.routing.backend=fix");
        }
        if (!omsConfig.getFix().isAutoStart()) {
            throw new IllegalStateException(
                    "Spring profile " + OmsProfiles.FIX_WORKER + " requires oms.fix.auto-start=true");
        }
        if (omsConfig.getControl().getPostgresWritePath() != ControlPostgresWritePath.INGRESS) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_WORKER
                            + " requires oms.control.postgres-write-path=ingress (no OrderIngress on this JVM; "
                            + "admission must run on ingress replicas — tail is dispatch-only).");
        }
        if (omsConfig.getChronicle().isEnabled() && !omsConfig.getChronicle().isControlTailEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_WORKER
                            + " requires oms.chronicle.control-tail-enabled=true (FIX-out JVM must consume the control Chronicle).");
        }
    }
}
