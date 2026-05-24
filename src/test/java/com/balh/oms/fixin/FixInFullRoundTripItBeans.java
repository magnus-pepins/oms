package com.balh.oms.fixin;

import com.balh.oms.fixegress.EgressBrokerEmbeddedAcceptor;
import com.balh.oms.fixegress.EgressBrokerFillingAcceptorApplication;
import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Wires embedded broker acceptor + FIX-in client initiator for {@link FixInFullRoundTripIT}. */
@TestConfiguration
public class FixInFullRoundTripItBeans {

    @Bean
    EgressBrokerFillingAcceptorApplication egressBrokerFillingAcceptorApplication() {
        return new EgressBrokerFillingAcceptorApplication();
    }

    @Bean
    EgressBrokerEmbeddedAcceptor egressBrokerEmbeddedAcceptor(
            @Value("${oms.fix.socket-connect-port}") int brokerPort,
            EgressBrokerFillingAcceptorApplication application) {
        return new EgressBrokerEmbeddedAcceptor(
                brokerPort, brokerStorePath(), application, "ACCEPTOR_FIR", "INITIATOR_FIR");
    }

    @Bean
    FixInClientEmbeddedInitiator fixInClientEmbeddedInitiator(@Value("${oms.fix-in.accept-port}") int acceptPort) {
        return new FixInClientEmbeddedInitiator(
                acceptPort, clientStorePath(), "LOOPBACK_CLIENT", "BALH_OMS");
    }

    private static Path brokerStorePath() {
        try {
            return Files.createTempDirectory("oms-fix-in-full-rt-broker-acc");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Path clientStorePath() {
        try {
            return Files.createTempDirectory("oms-fix-in-full-rt-client-ini");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
