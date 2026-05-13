package com.balh.oms.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;

/**
 * Fail-fast rules for {@value OmsProfiles#POSTGRES_PROJECTOR} (Phase 2 of
 * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>The projector JVM consumes the Aeron cluster log and writes Postgres projection rows. It is mutually exclusive
 * with the legacy worker / ingress profiles ({@link TopologyWorkerProfiles#validateNoConflictingWorkerProfiles}) and
 * must not enable order-accept transports — those are excluded from this JVM by the {@code ORDER_ACCEPT_PROFILE}
 * expression on {@code OrdersController}, {@code OrderIngressService}, {@code OmsClusterIngressClient}, etc.
 *
 * <p>Excluded from the {@code test} profile so Spring boot context tests can probe configuration without booting a
 * real cluster connection. Production deployments always carry the real validator.
 */
@Component
@Profile("!test")
public class PostgresProjectorTopologyValidator {

    private final Environment environment;
    private final OmsConfig omsConfig;

    public PostgresProjectorTopologyValidator(Environment environment, OmsConfig omsConfig) {
        this.environment = environment;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validatePostgresProjectorTopology(environment, omsConfig);
    }

    public static void validatePostgresProjectorTopology(Environment environment, OmsConfig omsConfig) {
        TopologyWorkerProfiles.validateNoConflictingWorkerProfiles(environment);
        if (!environment.acceptsProfiles(Profiles.of(OmsProfiles.POSTGRES_PROJECTOR))) {
            return;
        }
        if (omsConfig.getGrpc().isEnabled()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.POSTGRES_PROJECTOR
                            + " requires oms.grpc.enabled=false (no OrderIngress on this JVM; gRPC server wiring would fail).");
        }
        if ("fix".equalsIgnoreCase(omsConfig.getRouting().getBackend()) && omsConfig.getFix().isAutoStart()) {
            throw new IllegalStateException(
                    "Spring profile "
                            + OmsProfiles.POSTGRES_PROJECTOR
                            + " cannot run QuickFIX with oms.routing.backend=fix and oms.fix.auto-start=true "
                            + "(single SocketInitiator per route); use Spring profile "
                            + OmsProfiles.FIX_EGRESS
                            + " for the FIX-out JVM.");
        }
    }
}
