package com.balh.oms.fixegress;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link TestConfiguration} for {@link OmsFixEgressBrokerIT}: registers the loopback embedded
 * acceptor as a {@link org.springframework.context.SmartLifecycle} bean. The acceptor binds the
 * port assigned by {@code @DynamicPropertySource} before {@code FixInitiatorManager}
 * (phase 0) so the egress's outbound logon attempt has a peer to connect to.
 */
@TestConfiguration
public class OmsFixEgressBrokerItBeans {

    @Bean
    EgressBrokerCountingAcceptorApplication egressBrokerCountingAcceptorApplication() {
        return new EgressBrokerCountingAcceptorApplication();
    }

    @Bean
    EgressBrokerEmbeddedAcceptor egressBrokerEmbeddedAcceptor(
            @Value("${oms.fix.socket-connect-port}") int loopbackPort,
            EgressBrokerCountingAcceptorApplication application) {
        return new EgressBrokerEmbeddedAcceptor(loopbackPort, acceptorStorePath(), application);
    }

    private static Path acceptorStorePath() {
        try {
            return Files.createTempDirectory("oms-fix-egress-it-acc");
        } catch (IOException e) {
            throw new IllegalStateException("failed to create egress acceptor file store dir", e);
        }
    }
}
