package com.balh.oms.fix.it;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@TestConfiguration
public class FixRoundTripTestBeans {

    @Bean
    @Profile("fix-roundtrip-it")
    FixRoundTripEmbeddedAcceptor fixRoundTripEmbeddedAcceptor() {
        return new FixRoundTripEmbeddedAcceptor(FixRoundTripFixture.PORT, FixRoundTripFixture.ACCEPTOR_STORE);
    }
}
