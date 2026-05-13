package com.balh.oms.cluster.admin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.Executors;

/**
 * Phase 4 slice 4b: minimal Prometheus exposition for the {@code oms-cluster-node} JVM, which has
 * no Spring Boot Actuator (and per ADR 0001 §Discipline must not gain one — cluster-resident code
 * is plain Java + Agrona).
 *
 * <p>Wraps a {@link PrometheusMeterRegistry} and a JDK {@link HttpServer} serving a single
 * {@code GET /metrics} endpoint that returns the Prometheus text exposition format. The registry
 * is passed to {@link com.balh.oms.cluster.OmsAdmissionClusteredService} via its
 * {@code MeterRegistry} constructor overload so snapshot timers / counters / byte summaries land
 * in the same registry that {@code /metrics} scrapes.
 *
 * <h3>Lifecycle</h3>
 *
 * <p>{@link #OmsClusterNodeMetricsExporter(int)} binds the listener and starts a small fixed-size
 * worker pool. {@link #close()} stops the pool and the listener with a short grace period. The
 * bootstrap owns one instance for the lifetime of the JVM and starts it <em>before</em> the
 * cluster service container so meters pre-register against it; that way {@code /metrics} exposes
 * {@code oms_cluster_snapshot_*_total = 0} immediately on boot, before any snapshot has fired.
 *
 * <h3>Configuration</h3>
 *
 * <p>Listener port resolved from {@link #ENV_METRICS_PORT}; defaults to {@link #DEFAULT_METRICS_PORT}.
 * Other Spring JVMs in the topology run on {@code 8081} (oms-ingress-replica) and {@code 8082}
 * (oms-postgres-projector) so {@link #DEFAULT_METRICS_PORT} = {@code 8089} avoids collision when
 * everything runs on the same machine in dev. Bind address is {@code 0.0.0.0} (k8s pod IP); pass
 * port {@code 0} for an ephemeral port (used by {@link #boundPort()} in tests).
 */
public final class OmsClusterNodeMetricsExporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterNodeMetricsExporter.class);

    /** Env var: TCP port the {@code /metrics} listener binds to. */
    public static final String ENV_METRICS_PORT = "OMS_CLUSTER_NODE_METRICS_PORT";

    /** Default {@code /metrics} listener port for the cluster-node JVM. */
    public static final int DEFAULT_METRICS_PORT = 8089;

    /** {@code GET /metrics} path. */
    public static final String METRICS_PATH = "/metrics";

    /**
     * HttpServer accept-queue backlog. JDK's HttpServer treats 0 as system default; a small backlog
     * is plenty for a Prometheus scraper that polls one node every 15-60s.
     */
    private static final int HTTPSERVER_BACKLOG = 0;

    /**
     * Worker pool size. Prometheus typically scrapes serially per target, so concurrency above 2 is
     * wasted; keep it small to bound thread footprint on the cluster-node JVM.
     */
    private static final int WORKER_POOL_SIZE = 2;

    /** Grace period for the worker pool to drain when shutting the exporter down. */
    private static final long SHUTDOWN_GRACE_SECONDS = 1L;

    private final PrometheusMeterRegistry meterRegistry;
    private final HttpServer httpServer;
    private final java.util.concurrent.ExecutorService workerPool;
    private final int boundPort;

    /**
     * @param port TCP port to bind. Pass {@code 0} for an ephemeral port (resolve via {@link #boundPort()}).
     */
    public OmsClusterNodeMetricsExporter(int port) {
        this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            this.httpServer = HttpServer.create(new InetSocketAddress(port), HTTPSERVER_BACKLOG);
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "failed to bind cluster-node metrics exporter on port %d", port), e);
        }
        this.workerPool = Executors.newFixedThreadPool(WORKER_POOL_SIZE, r -> {
            Thread t = new Thread(r, "oms-cluster-node-metrics-exporter");
            t.setDaemon(true);
            return t;
        });
        this.httpServer.createContext(METRICS_PATH, this::handleMetrics);
        this.httpServer.setExecutor(workerPool);
        this.httpServer.start();
        this.boundPort = httpServer.getAddress().getPort();
        log.info("OmsClusterNodeMetricsExporter listening on http://0.0.0.0:{}{}", boundPort, METRICS_PATH);
    }

    /** @return the registry meters register against (and that {@code /metrics} scrapes). */
    public MeterRegistry meterRegistry() {
        return meterRegistry;
    }

    /** @return the actually-bound port (useful when port {@code 0} was requested). */
    public int boundPort() {
        return boundPort;
    }

    /**
     * Resolves the listener port from {@link #ENV_METRICS_PORT}; falls back to
     * {@link #DEFAULT_METRICS_PORT} when unset / blank. Throws if the env var is set but not a
     * valid TCP port.
     */
    public static int resolveMetricsPortFromEnv() {
        String raw = System.getenv(ENV_METRICS_PORT);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_METRICS_PORT;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid %s='%s'; expected a TCP port (0-65535)", ENV_METRICS_PORT, raw),
                    e);
        }
        if (parsed < 0 || parsed > 0xFFFF) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid %s='%s'; out of range 0-65535", ENV_METRICS_PORT, raw));
        }
        return parsed;
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            byte[] body = meterRegistry.scrape().getBytes(StandardCharsets.UTF_8);
            // Prometheus 0.0.4 text exposition is the canonical Micrometer scrape format.
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }

    @Override
    public void close() {
        // Stop accepting new requests immediately; in-flight handlers get up to SHUTDOWN_GRACE_SECONDS to drain
        // before HttpServer is forced closed. The grace is short because /metrics handlers do nothing but
        // call PrometheusMeterRegistry.scrape() which is memory-bound and completes in milliseconds.
        try {
            httpServer.stop((int) SHUTDOWN_GRACE_SECONDS);
        } catch (RuntimeException e) {
            log.warn("error stopping cluster-node metrics HttpServer", e);
        }
        workerPool.shutdownNow();
        try {
            meterRegistry.close();
        } catch (RuntimeException e) {
            log.warn("error closing cluster-node Prometheus meter registry", e);
        }
        log.info("OmsClusterNodeMetricsExporter stopped (was on http://0.0.0.0:{}{})", boundPort, METRICS_PATH);
    }
}
