package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * Phase 3 entrypoint of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: same
 * Spring Boot application as {@link OmsApplication} with profile
 * {@value OmsProfiles#FIX_EGRESS} added.
 *
 * <p>Slice 3a runs the skeleton bean only (no cluster consumption, no QuickFIX yet). Slice 3b
 * wires Aeron Archive replay → {@code Session.sendToTarget(NewOrderSingle)} and hosts the
 * QuickFIX {@code SocketInitiator} (so the legacy {@link OmsFixWorkerBootstrap} role retires
 * once slice 3g lands).
 *
 * <p>Local: {@code ./gradlew bootRunFixEgress}. Containers may also use
 * {@code java -jar oms.jar --spring.profiles.include=oms-fix-egress}.
 */
public final class OmsFixEgressBootstrap {

    private OmsFixEgressBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.FIX_EGRESS);
        application.run(args);
    }
}
