package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for {@value OmsProfiles#FIX_EGRESS} (Phase 3 of
 * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>The FIX-egress JVM owns QuickFIX/J's {@code SocketInitiator} (broker constraint: exactly one per route) and
 * reads the cluster events recording directly via Aeron Archive replay (mirrors {@link PostgresProjectorTopologyValidator}
 * on the FIX side). It is mutually exclusive with every other role profile
 * ({@link TopologyWorkerProfiles#validateNoConflictingWorkerProfiles}) and must not enable order-accept transports —
 * the {@code ORDER_ACCEPT_PROFILE} expression already excludes {@code oms-fix-egress}, but we double-check the
 * runtime invariants here so a bad config produces a startup failure with a clear message instead of a half-loaded
 * context.
 *
 * <p>Unlike the projector validator, this role <strong>does</strong> own QuickFIX: {@code oms.routing.backend=fix}
 * and {@code oms.fix.auto-start=true} are both expected. We reject {@code oms.grpc.enabled=true} (no order
 * ingress on this JVM).
 *
 * <p><strong>Slice 3d:</strong> {@code oms.cluster.client.enabled=true} is now <em>required</em>: the egress
 * JVM offers {@link com.balh.oms.cluster.ApplyExecutionReportCommand} back to the cluster on inbound FIX
 * {@code ExecutionReport} / {@code OrderCancelReject}. The earlier slice (3a/3b) rejected the property to
 * mark the role's read-only intent on the events recording; that constraint is gone now that the role also
 * writes back. Misconfigured deployments (cluster-client off) would silently swallow inbound venue traffic,
 * so we fail fast at startup.
 *
 * <p>Excluded from the {@code test} profile so Spring boot context tests can probe configuration without booting a
 * real cluster connection. Production deployments always carry the real validator.
 */
@Component
@Profile("!test")
public class FixEgressTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public FixEgressTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validateFixEgressTopology(environment, omsConfig);
    }

    public static void validateFixEgressTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.FIX_EGRESS))) {
            return;
        }
        if (omsConfig.getGrpc().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_EGRESS
                            + " requires oms.grpc.enabled=false (no OrderIngress on this JVM; gRPC server wiring would fail).");
        }
        if (!omsConfig.getCluster().getClient().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.FIX_EGRESS
                            + " requires oms.cluster.client.enabled=true (slice 3d: inbound ExecutionReport /"
                            + " OrderCancelReject is translated to ApplyExecutionReportCommand and offered to the"
                            + " cluster via OmsClusterIngressClient; without it the inbound venue traffic would be"
                            + " silently dropped).");
        }
    }
}
