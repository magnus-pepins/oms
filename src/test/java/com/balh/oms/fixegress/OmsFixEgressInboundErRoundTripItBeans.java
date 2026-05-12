package com.balh.oms.fixegress;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Slice 3d round-trip wiring: replaces the slice-3b-2 counting acceptor with a filling acceptor
 * that synthesises an ER reply on every NOS receipt. The {@link EgressBrokerEmbeddedAcceptor}
 * lifecycle bean is reused unchanged — it is parameterised on the {@link quickfix.Application}
 * implementation, so the only difference between the slice-3b-2 IT and this IT is which {@code
 * Application} bean the acceptor binds.
 */
@TestConfiguration
public class OmsFixEgressInboundErRoundTripItBeans {

    @Bean
    EgressBrokerFillingAcceptorApplication egressBrokerFillingAcceptorApplication() {
        return new EgressBrokerFillingAcceptorApplication();
    }

    @Bean
    EgressBrokerEmbeddedAcceptor egressBrokerEmbeddedAcceptor(
            @Value("${oms.fix.socket-connect-port}") int loopbackPort,
            EgressBrokerFillingAcceptorApplication application) {
        // Distinct CompIDs match the OmsFixEgressInboundErRoundTripIT initiator settings; see the
        // class-level note there on QuickFIX/J's JVM-global session registry.
        return new EgressBrokerEmbeddedAcceptor(
                loopbackPort, acceptorStorePath(), application, "ACCEPTOR_RT", "INITIATOR_RT");
    }

    private static Path acceptorStorePath() {
        try {
            return Files.createTempDirectory("oms-fix-egress-roundtrip-it-acc");
        } catch (IOException e) {
            throw new IllegalStateException("failed to create round-trip acceptor file store dir", e);
        }
    }
}
