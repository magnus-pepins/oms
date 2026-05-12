package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * Phase 2 entrypoint of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: same Spring Boot
 * application as {@link OmsApplication} with profile {@value OmsProfiles#POSTGRES_PROJECTOR} added.
 *
 * <p>Slice 2a runs only the cursor table + skeleton bean (no event consumption yet). Slice 2b wires consumption
 * from the cluster log; slice 2c removes the dual-write from {@code OrderIngressService}.
 *
 * <p>Local: {@code ./gradlew bootRunPostgresProjector}. Containers may also use
 * {@code java -jar oms.jar --spring.profiles.include=oms-postgres-projector}.
 */
public final class OmsPostgresProjectorBootstrap {

    private OmsPostgresProjectorBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.POSTGRES_PROJECTOR);
        application.run(args);
    }
}
