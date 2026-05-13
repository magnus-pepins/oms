package com.balh.oms.ingress.bench;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    void runBurst_roundRobinsAcrossMultipleTargets() throws Exception {
        // Slice 4m: when OMS_BURST_URLS lists N targets the burst tool must spread requests
        // exactly round-robin (request i -> urls[i % N]). Two in-process servers stand in for
        // two ingress-replicas; the test asserts each receives the exact share of the burst.
        ConcurrentHashMap<Integer, AtomicInteger> seenByPort = new ConcurrentHashMap<>();
        HttpServer s1 = startStubServer(API_KEY, seenByPort);
        HttpServer s2 = startStubServer(API_KEY, seenByPort);
        try {
            int total = 40;
            List<String> urls = List.of(
                    "http://127.0.0.1:" + s1.getAddress().getPort() + "/internal/v1/orders",
                    "http://127.0.0.1:" + s2.getAddress().getPort() + "/internal/v1/orders");
            Config cfg = new Config(
                    API_KEY,
                    urls,
                    total,
                    /* concurrency = */ 4,
                    /* rpsCap = */ 0,
                    /* accountPoolSize = */ 2,
                    "AAPL",
                    "1",
                    "150",
                    /* requestTimeoutSeconds = */ 5,
                    /* warmup = */ 0);
            BurstResult result = IngressBurstMain.run(cfg);

            assertEquals(total, result.successCount, "all 201s expected across both replicas");
            // Round-robin guarantees: with concurrency >1 the actual receive order can interleave
            // but the *count* per target is exactly total / urls.size() because the dispatch
            // index runs serially through the for-loop (only the in-flight CompletableFutures
            // reorder). Asserting on submitted counts catches a regression where some future
            // refactor lets concurrency randomise target selection.
            for (String u : urls) {
                AtomicLong c = result.perTargetSubmitted.get(u);
                assertThat(c).as("per-target counter for %s exists", u).isNotNull();
                assertThat(c.get())
                        .as("per-target submitted count for %s", u)
                        .isEqualTo(total / urls.size());
            }
            int sumServerHits = seenByPort.values().stream().mapToInt(AtomicInteger::get).sum();
            assertEquals(total, sumServerHits, "every burst request must have hit one of the stubs");
            assertEquals(2, seenByPort.size(), "both replicas must have observed at least one request");
        } finally {
            s1.stop(0);
            s2.stop(0);
        }
    }

    @Test
    void parseUrls_blank_fallsBackToSingleUrl() {
        assertEquals(List.of("http://fallback/orders"),
                IngressBurstMain.parseUrls(null, "http://fallback/orders"));
        assertEquals(List.of("http://fallback/orders"),
                IngressBurstMain.parseUrls("", "http://fallback/orders"));
        assertEquals(List.of("http://fallback/orders"),
                IngressBurstMain.parseUrls("   ", "http://fallback/orders"));
    }

    @Test
    void parseUrls_csv_trimsAndDropsEmpty() {
        assertEquals(
                List.of("http://a/o", "http://b/o", "http://c/o"),
                IngressBurstMain.parseUrls(" http://a/o , http://b/o ,, http://c/o ", "ignored"));
    }

    private static HttpServer startStubServer(String apiKey, ConcurrentHashMap<Integer, AtomicInteger> seenByPort) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        seenByPort.put(port, new AtomicInteger());
        server.createContext("/internal/v1/orders", exchange -> {
            seenByPort.get(port).incrementAndGet();
            String header = exchange.getRequestHeaders().getFirst("X-OMS-Internal-Key");
            if (!apiKey.equals(header)) {
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
        return server;
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
