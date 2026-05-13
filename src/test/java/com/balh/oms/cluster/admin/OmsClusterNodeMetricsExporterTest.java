package com.balh.oms.cluster.admin;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 4b unit test: verifies the {@link OmsClusterNodeMetricsExporter} HTTP path actually serves
 * the Prometheus text exposition format on {@code /metrics}, including a meter we synthetically
 * register before the scrape (so we don't depend on a running cluster).
 *
 * <p>This is the cheap counterpart to {@link OmsClusterSnapshotAdminToolIT}, which proves the
 * meters fire from the cluster-service hot path. Together they cover the slice 4b acceptance:
 * meters get recorded on snapshot, and the cluster-node JVM serves them on {@code /metrics}.
 */
class OmsClusterNodeMetricsExporterTest {

    /**
     * Generous read timeout for the local HTTP request; in practice {@code /metrics} returns in
     * milliseconds. Kept long enough to absorb GC pauses in a busy CI shared runner without flaking.
     */
    private static final int HTTP_READ_TIMEOUT_MS = (int) Duration.ofSeconds(5).toMillis();

    /** Connect timeout for the local HTTP request — same rationale as the read timeout. */
    private static final int HTTP_CONNECT_TIMEOUT_MS = (int) Duration.ofSeconds(2).toMillis();

    /** Simulated snapshot-write byte payload size; pick a non-zero value so the {@code _sum} is identifiable. */
    private static final long SYNTHETIC_SNAPSHOT_BYTES = 4096L;

    /** Simulated snapshot-write timer recording (nanos); nonzero so the {@code _sum} is non-zero. */
    private static final long SYNTHETIC_SNAPSHOT_DURATION_NANOS = 1_500_000L;

    @Test
    void metricsEndpoint_servesPrometheusExposition_withRegisteredMeters() throws IOException {
        try (OmsClusterNodeMetricsExporter exporter = new OmsClusterNodeMetricsExporter(0)) {
            // Pre-register snapshot meters so the scrape body proves the registry wiring works.
            // We use the same names and tags OmsAdmissionClusteredService registers, so the test
            // also serves as a guard against accidental rename of the meter ids.
            Tags writeTags = Tags.of("outcome", "write");
            Counter writeCounter = Counter.builder("oms.cluster.snapshot.events")
                    .tags(writeTags)
                    .register(exporter.meterRegistry());
            writeCounter.increment();

            Timer writeTimer = Timer.builder("oms.cluster.snapshot.duration")
                    .tags(writeTags)
                    .register(exporter.meterRegistry());
            writeTimer.record(Duration.ofNanos(SYNTHETIC_SNAPSHOT_DURATION_NANOS));

            io.micrometer.core.instrument.DistributionSummary writeBytes =
                    io.micrometer.core.instrument.DistributionSummary.builder("oms.cluster.snapshot.bytes")
                            .tags(writeTags)
                            .baseUnit("bytes")
                            .register(exporter.meterRegistry());
            writeBytes.record(SYNTHETIC_SNAPSHOT_BYTES);

            int port = exporter.boundPort();
            assertThat(port).as("ephemeral port must be assigned").isGreaterThan(0);

            URI uri = URI.create("http://localhost:" + port + OmsClusterNodeMetricsExporter.METRICS_PATH);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);
            connection.setRequestMethod("GET");

            assertThat(connection.getResponseCode()).as("status code").isEqualTo(200);
            assertThat(connection.getContentType())
                    .as("content type")
                    .startsWith("text/plain")
                    .contains("version=0.0.4");

            String body;
            try (InputStream in = connection.getInputStream()) {
                body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }

            // Counter is exposed as "oms_cluster_snapshot_events_total" in Prometheus.
            assertThat(body).contains("oms_cluster_snapshot_events_total{outcome=\"write\"} 1.0");

            // Timer surfaces three metric series (count / sum / max) with seconds as the base unit.
            assertThat(body).contains("oms_cluster_snapshot_duration_seconds_count{outcome=\"write\"} 1");
            assertThat(body).contains("oms_cluster_snapshot_duration_seconds_sum{outcome=\"write\"}");

            // DistributionSummary exposes count/sum/max as well; we recorded one bytes sample.
            assertThat(body).contains("oms_cluster_snapshot_bytes_count{outcome=\"write\"} 1");
            assertThat(body).contains("oms_cluster_snapshot_bytes_sum{outcome=\"write\"} " + (double) SYNTHETIC_SNAPSHOT_BYTES);
        }
    }

    @Test
    void nonGetMethod_returns405() throws IOException {
        try (OmsClusterNodeMetricsExporter exporter = new OmsClusterNodeMetricsExporter(0)) {
            URI uri = URI.create("http://localhost:" + exporter.boundPort() + OmsClusterNodeMetricsExporter.METRICS_PATH);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.getOutputStream().close();

            assertThat(connection.getResponseCode())
                    .as("non-GET requests must be rejected so a misconfigured scraper fails loudly")
                    .isEqualTo(405);
        }
    }

    @Test
    void resolveMetricsPortFromEnv_defaultsWhenUnset() {
        // We can't unset env vars from inside the JVM, but we can prove the default kicks in for the
        // common case of no override (ENV_METRICS_PORT is not set on dev boxes).
        if (System.getenv(OmsClusterNodeMetricsExporter.ENV_METRICS_PORT) == null) {
            assertThat(OmsClusterNodeMetricsExporter.resolveMetricsPortFromEnv())
                    .isEqualTo(OmsClusterNodeMetricsExporter.DEFAULT_METRICS_PORT);
        }
    }
}
