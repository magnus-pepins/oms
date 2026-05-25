package com.balh.oms.fixin;

import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@TestConfiguration
public class FixInToVenueRoundTripItBeans {

    @Bean
    FixInClientEmbeddedInitiator fixInClientEmbeddedInitiator(@Value("${oms.fix-in.accept-port}") int acceptPort) {
        return new FixInClientEmbeddedInitiator(
                acceptPort, clientStorePath(), "LOOPBACK_CLIENT", "BALH_OMS");
    }

    private static Path clientStorePath() {
        try {
            return Files.createTempDirectory("oms-fix-in-venue-rt-client-ini");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
