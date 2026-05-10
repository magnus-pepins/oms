package com.balh.oms.fix.it;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared file-store dirs for FIX round-trip integration tests.
 *
 * <p>TCP port is <strong>not</strong> global: each {@code @SpringBootTest} context registers its
 * own loopback port via {@link FixRoundTripDynamicProperties#registerLoopbackPort} so embedded
 * acceptors do not collide under Spring context caching.
 */
public final class FixRoundTripFixture {

    public static final Path INITIATOR_STORE;
    public static final Path ACCEPTOR_STORE;

    static {
        try {
            INITIATOR_STORE = Files.createTempDirectory("oms-fix-it-ini");
            ACCEPTOR_STORE = Files.createTempDirectory("oms-fix-it-acc");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private FixRoundTripFixture() {
    }
}
