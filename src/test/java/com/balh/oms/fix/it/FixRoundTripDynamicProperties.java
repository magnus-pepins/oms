package com.balh.oms.fix.it;

import org.springframework.test.context.DynamicPropertyRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

/**
 * Registers a <strong>fresh TCP port per Spring test context</strong> for the FIX round-trip
 * loopback profile.
 *
 * <p>Previously {@link FixRoundTripFixture} used a single JVM-wide port chosen once in a static
 * block (after closing the probing {@link ServerSocket}). Spring's test {@code ApplicationContext}
 * cache can keep several FIX round-trip contexts alive; each embeds an acceptor that must bind
 * the same port the initiator connects to. Reusing one port across contexts causes
 * {@code BindException: Address already in use} on CI.
 */
public final class FixRoundTripDynamicProperties {

    /**
     * Binds an ephemeral port once (per {@link org.springframework.test.context.DynamicPropertySource}
     * invocation), closes the probe socket, then registers that port for {@code oms.fix.socket-connect-port}.
     * Sequential Spring context creation still leaves a narrow TOCTOU window; JUnit parallel is
     * disabled for this module, and each distinct {@code DynamicPropertySource} runs when its
     * context is first created, yielding a distinct port per cached context key.
     */
    public static void registerLoopbackPort(DynamicPropertyRegistry registry) {
        final int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to allocate loopback port for FIX IT", e);
        }
        registry.add("oms.fix.socket-connect-port", () -> String.valueOf(port));
    }

    private FixRoundTripDynamicProperties() {}
}
