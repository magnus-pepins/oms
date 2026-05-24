package com.balh.oms.config;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/** Shared validation for mutually exclusive Spring worker profiles. */
public final class TopologyWorkerProfiles {

    private TopologyWorkerProfiles() {}

    public static void validateNoConflictingWorkerProfiles(Environment environment) {
        // oms-postgres-projector is a dedicated JVM role (Phase 2 of the Aeron Cluster substrate plan): it consumes
        // the cluster log and writes Postgres projections. It must not co-run with the ingress JVM because that one
        // loads order-accept beans which would conflict with the projector's lifecycle.
        rejectPair(environment, OmsProfiles.POSTGRES_PROJECTOR, OmsProfiles.INGRESS_REPLICA);
        // oms-fix-egress (Phase 3 slice 3a of the Aeron Cluster substrate plan): the FIX-out JVM that owns QuickFIX
        // and reads the cluster events recording directly. Mutex with every other role profile because it owns the
        // singleton FIX SocketInitiator (broker constraint: one initiator per route) and runs the egress-cursor
        // replay loop, neither of which can share a JVM with the other roles.
        rejectPair(environment, OmsProfiles.FIX_EGRESS, OmsProfiles.INGRESS_REPLICA);
        rejectPair(environment, OmsProfiles.FIX_EGRESS, OmsProfiles.POSTGRES_PROJECTOR);
        rejectPair(environment, OmsProfiles.FIX_INGRESS, OmsProfiles.INGRESS_REPLICA);
        rejectPair(environment, OmsProfiles.FIX_INGRESS, OmsProfiles.POSTGRES_PROJECTOR);
        rejectPair(environment, OmsProfiles.FIX_INGRESS, OmsProfiles.FIX_EGRESS);
    }

    private static void rejectPair(Environment environment, String profileA, String profileB) {
        if (environment.acceptsProfiles(Profiles.of(profileA))
                && environment.acceptsProfiles(Profiles.of(profileB))) {
            throw new IllegalStateException(
                    "Cannot activate Spring profiles " + profileA + " and " + profileB + " on the same JVM.");
        }
    }
}
