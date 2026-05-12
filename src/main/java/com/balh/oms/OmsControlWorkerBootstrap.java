package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * P3 prep entrypoint: same Spring Boot application as {@link OmsApplication} with profile
 * {@value OmsProfiles#CONTROL_WORKER} added (no new-order HTTP/gRPC ingress; Chronicle append via reconciler per
 * {@code application-oms-control-worker.yaml}). Use for classpath launches without setting {@code SPRING_PROFILES_ACTIVE}.
 *
 * <p>Fat JAR {@code Start-Class} remains {@link OmsApplication}; containers typically use {@code java -jar oms.jar
 * --spring.profiles.include=oms-control-worker} (or {@code active=}) instead. Local: {@code ./gradlew bootRunControlWorker}.
 */
public final class OmsControlWorkerBootstrap {

    private OmsControlWorkerBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.CONTROL_WORKER);
        application.run(args);
    }
}
