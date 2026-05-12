package com.balh.oms.config;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/** Shared validation for mutually exclusive Spring worker profiles. */
public final class TopologyWorkerProfiles {

    private TopologyWorkerProfiles() {}

    public static void validateNoConflictingWorkerProfiles(Environment environment) {
        if (environment.acceptsProfiles(Profiles.of(OmsProfiles.CONTROL_WORKER))
                && environment.acceptsProfiles(Profiles.of(OmsProfiles.FIX_WORKER))) {
            throw new IllegalStateException(
                    "Cannot activate Spring profiles "
                            + OmsProfiles.CONTROL_WORKER
                            + " and "
                            + OmsProfiles.FIX_WORKER
                            + " on the same JVM.");
        }
        if (environment.acceptsProfiles(Profiles.of(OmsProfiles.INGRESS_REPLICA))
                && environment.acceptsProfiles(Profiles.of(OmsProfiles.CONTROL_WORKER))) {
            throw new IllegalStateException(
                    "Cannot activate Spring profiles "
                            + OmsProfiles.INGRESS_REPLICA
                            + " and "
                            + OmsProfiles.CONTROL_WORKER
                            + " on the same JVM.");
        }
        if (environment.acceptsProfiles(Profiles.of(OmsProfiles.INGRESS_REPLICA))
                && environment.acceptsProfiles(Profiles.of(OmsProfiles.FIX_WORKER))) {
            throw new IllegalStateException(
                    "Cannot activate Spring profiles "
                            + OmsProfiles.INGRESS_REPLICA
                            + " and "
                            + OmsProfiles.FIX_WORKER
                            + " on the same JVM.");
        }
    }
}
