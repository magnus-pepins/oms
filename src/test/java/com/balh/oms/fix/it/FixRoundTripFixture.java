package com.balh.oms.fix.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared loopback port and file-store dirs for {@link FixRoundTripSpringIntegrationTest}.
 */
public final class FixRoundTripFixture {

    public static final int PORT;
    public static final Path INITIATOR_STORE;
    public static final Path ACCEPTOR_STORE;

    static {
        try (ServerSocket socket = new ServerSocket(0)) {
            PORT = socket.getLocalPort();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
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
