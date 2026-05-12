package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * P4 prep entrypoint: same Spring Boot application as {@link OmsApplication} with profile
 * {@value OmsProfiles#FIX_WORKER} added (no new-order HTTP/gRPC ingress; reconciler Chronicle append; QuickFIX
 * initiator allowed per {@code application-oms-fix-worker.yaml}).
 *
 * <p>Fat JAR {@code Start-Class} remains {@link OmsApplication}; containers can use {@code java -jar oms.jar
 * --spring.profiles.include=oms-fix-worker}. Local: {@code ./gradlew bootRunFixWorker}.
 */
public final class OmsFixWorkerBootstrap {

    private OmsFixWorkerBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.FIX_WORKER);
        application.run(args);
    }
}
