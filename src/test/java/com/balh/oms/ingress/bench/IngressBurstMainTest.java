package com.balh.oms.ingress.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.balh.oms.ingress.bench.IngressBurstMain.BurstResult;
import com.balh.oms.ingress.bench.IngressBurstMain.Config;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Smoke test for {@link IngressBurstMain}. Boots a tiny in-process {@link HttpServer} that
 * accepts {@code POST /internal/v1/orders} and replies 201, then verifies the burst tool's
 * outcome counters and HdrHistogram match the request count. Deliberately avoids touching
 * Aeron / Postgres so this runs as a pure unit test in CI.
 */
final class IngressBurstMainTest {

    private static final String API_KEY = "burst-test-key";
    private static final int TOTAL = 50;
    private static final int CONCURRENCY = 8;

    @Test
    void runBurst_recordsAllSuccesses() throws Exception {
        AtomicInteger seenRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/orders", exchange -> {
            seenRequests.incrementAndGet();
            String header = exchange.getRequestHeaders().getFirst("X-OMS-Internal-Key");
            if (!API_KEY.equals(header)) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            byte[] body = "".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            Config cfg = new Config(
                    API_KEY,
                    "http://127.0.0.1:" + port + "/internal/v1/orders",
                    TOTAL,
                    CONCURRENCY,
                    /* rpsCap = */ 0,
                    /* accountPoolSize = */ 4,
                    /* instrument = */ "AAPL",
                    /* quantity = */ "1",
                    /* limitPrice = */ "150",
                    /* requestTimeoutSeconds = */ 5,
                    /* warmup = */ 0);
            BurstResult result = IngressBurstMain.run(cfg);
            assertEquals(TOTAL, result.submitted, "every request must complete");
            assertEquals(TOTAL, result.successCount, "all 201s expected");
            assertEquals(TOTAL, result.createdCount, "all responses are 201");
            assertEquals(0L, result.duplicateCount);
            assertEquals(0L, result.failureCount);
            assertEquals(TOTAL, result.steadyHistogram.getTotalCount());
            assertTrue(result.steadyHistogram.getMinValue() > 0L,
                    "histogram should record positive RTT");
            assertEquals(TOTAL, seenRequests.get(),
                    "test server must have observed every burst request");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void config_rejectsMissingApiKey() {
        assertThrows(IllegalArgumentException.class,
                () -> new Config(
                        "",
                        "http://localhost/orders",
                        1,
                        1,
                        0,
                        1,
                        "AAPL",
                        "1",
                        "1",
                        1,
                        0));
    }

    @Test
    void config_rejectsInvalidNumerics() {
        assertThrows(IllegalArgumentException.class,
                () -> new Config(
                        "k",
                        "http://localhost/orders",
                        /* total = */ 0,
                        1,
                        0,
                        1,
                        "AAPL",
                        "1",
                        "1",
                        1,
                        0));
        assertThrows(IllegalArgumentException.class,
                () -> new Config(
                        "k",
                        "http://localhost/orders",
                        1,
                        /* concurrency = */ 0,
                        0,
                        1,
                        "AAPL",
                        "1",
                        "1",
                        1,
                        0));
        assertThrows(IllegalArgumentException.class,
                () -> new Config(
                        "k",
                        "http://localhost/orders",
                        1,
                        1,
                        /* rpsCap = */ -1,
                        1,
                        "AAPL",
                        "1",
                        "1",
                        1,
                        0));
        assertThrows(IllegalArgumentException.class,
                () -> new Config(
                        "k",
                        "http://localhost/orders",
                        2,
                        1,
                        0,
                        1,
                        "AAPL",
                        "1",
                        "1",
                        1,
                        /* warmup = */ 3));
    }
}
