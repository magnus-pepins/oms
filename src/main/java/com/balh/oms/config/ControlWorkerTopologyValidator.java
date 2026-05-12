package com.balh.oms.config;

import com.balh.oms.chronicle.ControlChronicleAppendMode;
import jakarta.annotation.PostConstruct;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * {@value OmsProfiles#CONTROL_WORKER} has no {@link com.balh.oms.ingress.OrderIngressService}, so
 * {@value ControlChronicleAppendMode#INGRESS_AFTER_COMMIT} would never append and would disable {@code OutboxReconciler}.
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
        if (omsConfig.getControl().isChronicleAppendIngressAfterCommit()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.CONTROL_WORKER
                            + " is incompatible with oms.control.chronicle-append-mode="
                            + ControlChronicleAppendMode.INGRESS_AFTER_COMMIT
                            + " (no local order accept to run afterCommit); use "
                            + ControlChronicleAppendMode.RECONCILER);
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
