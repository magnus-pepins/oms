package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.SpringApplication;

/**
 * Entrypoint for the FIX-in acceptor role ({@code oms-fix-ingress}): QuickFIX/J
 * {@code SocketAcceptor} that admits {@code NewOrderSingle} via {@code AcceptOrderCommand}.
 *
 * <p>Local: {@code ./gradlew bootRunFixIngress}.
 */
public final class OmsFixIngressBootstrap {

    private OmsFixIngressBootstrap() {}

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(OmsApplication.class);
        application.setAdditionalProfiles(OmsProfiles.FIX_INGRESS);
        application.run(args);
    }
}
