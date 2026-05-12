package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for the {@value OmsProfiles#CONTROL_WORKER} JVM (no {@link
 * com.balh.oms.ingress.OrderIngressService}). Phase 1c slice C deletes the
 * {@code chronicle-append-mode} switch this validator used to gate on; the remaining checks
 * below are now the only invariants.
 */
@Component
public class ControlWorkerTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public ControlWorkerTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validateTopology(environment, omsConfig);
    }

    /**
     * Fail-fast rules for {@value OmsProfiles#CONTROL_WORKER} (invoked from {@link PostConstruct} and unit tests).
     */
    public static void validateTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.CONTROL_WORKER))) {
            return;
        }
        if (omsConfig.getGrpc().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.CONTROL_WORKER
                            + " requires oms.grpc.enabled=false (no OrderIngress on this JVM; gRPC server wiring would fail).");
        }
        if ("fix".equalsIgnoreCase(omsConfig.getRouting().getBackend()) && omsConfig.getFix().isAutoStart()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.CONTROL_WORKER
                            + " cannot run QuickFIX with oms.routing.backend=fix and oms.fix.auto-start=true "
                            + "(single SocketInitiator per route); use Spring profile "
                            + OmsProfiles.FIX_WORKER
                            + " for the FIX-out JVM.");
        }
        if (omsConfig.getChronicle().isEnabled() && !omsConfig.getChronicle().isControlTailEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.CONTROL_WORKER
                            + " requires oms.chronicle.control-tail-enabled=true (this JVM must consume the control Chronicle).");
        }
    }
}
