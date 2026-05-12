package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * P5 prep entrypoint: same Spring Boot application as {@link OmsApplication} with profile
 * {@value OmsProfiles#INGRESS_REPLICA} added (order accept + {@code postgres-write-path=ingress}; no local control tail
 * per {@code application-oms-ingress-replica.yaml}). Use for classpath launches without setting
 * {@code SPRING_PROFILES_ACTIVE}.
 *
 * <p>Fat JAR {@code Start-Class} is this class; the default monolith JAR remains {@link OmsApplication}. Containers
 * may also use {@code java -jar oms.jar --spring.profiles.include=oms-ingress-replica}. Local:
 * {@code ./gradlew bootRunIngressReplica}.
 */
public final class OmsIngressReplicaBootstrap {

    private OmsIngressReplicaBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.INGRESS_REPLICA);
        application.run(args);
    }
}
