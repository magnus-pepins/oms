package com.balh.oms.fix.it;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@TestConfiguration
public class FixRoundTripTestBeans {

    @Bean
    @Profile("fix-roundtrip-it")
    FixRoundTripEmbeddedAcceptor fixRoundTripEmbeddedAcceptor(
            @Value("${oms.fix.socket-connect-port}") int loopbackPort) {
        return new FixRoundTripEmbeddedAcceptor(loopbackPort, FixRoundTripFixture.ACCEPTOR_STORE);
    }
}
