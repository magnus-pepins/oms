package com.balh.oms.cluster;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.OmsClusterNodeBootstrap;
import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 closeout integration test for the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
 *
 * <p>Boots an in-process single-node Aeron Cluster, then runs Spring Boot
 * with {@code oms.cluster.client.enabled=true} pointed at it. Verifies the
 * full HTTP → cluster admission → Postgres path:
 *
 * <ul>
 *   <li>Fresh {@code POST /internal/v1/orders} commits via the cluster and
 *       writes the same {@code orders} / {@code control_outbox} /
 *       {@code domain_event_outbox} rows the legacy Chronicle path produced.</li>
 *   <li>A duplicate post (same {@code accountId} + {@code clientIdempotencyKey})
 *       is detected by the cluster, returns the original {@code orderId}, and
 *       does not create a second {@code orders} row (option (A): cluster gate
 *       is authoritative; Postgres write stays in the ingress transaction in
 *       Phase 1, projection split-out lands in Phase 2).</li>
 * </ul>
 *
 * <p>Complement to {@link OmsClusterIngressClientIT} (cluster client in
 * isolation) and {@link com.balh.oms.ingress.OrderIngressServiceClusterGateTest}
 * (Mockito unit test of the gate logic): this is the only test that wires
 * controller + service + cluster client + actual Aeron Cluster + Postgres
 * end-to-end.
 */
class OrderIngressClusterIntegrationTest extends AbstractPostgresIntegrationTest {

    /**
     * Dedicated port set so this cluster cannot collide with the defaults
     * used in {@link OmsClusterIngressClientIT} or
     * {@link com.balh.oms.OmsClusterNodeBootstrapSmokeIT} when Gradle reuses
     * a single test JVM across classes.
     */
    private static final int INGRESS_PORT = 20510;
    private static final int CONSENSUS_PORT = 20520;
    private static final int LOG_PORT = 20530;
    private static final int CATCHUP_PORT = 20540;
    private static final int ARCHIVE_PORT = 20550;

    private static final String CLUSTER_MEMBERS_SINGLE_NODE = "0,localhost:" + INGRESS_PORT
            + ",localhost:" + CONSENSUS_PORT
            + ",localhost:" + LOG_PORT
            + ",localhost:" + CATCHUP_PORT
            + ",localhost:" + ARCHIVE_PORT;

    private static final String CLUSTER_INGRESS_ENDPOINTS = "0=localhost:" + INGRESS_PORT;

    private static final String CLUSTER_ARCHIVE_CONTROL_CHANNEL =
            "aeron:udp?endpoint=localhost:" + ARCHIVE_PORT;

    private static final OmsClusterNodeBootstrap.ClusterNodePaths PATHS;

    static {
        try {
            Path baseDir = Files.createTempDirectory("oms-phase1-it-cluster-");
            PATHS = new OmsClusterNodeBootstrap.ClusterNodePaths(
                    baseDir.toString(),
                    baseDir.resolve("media-driver").toString(),
                    baseDir.resolve("archive").toString(),
                    baseDir.resolve("consensus-module").toString(),
                    baseDir.resolve("cluster-services").toString());
        } catch (IOException e) {
            throw new UncheckedIOException("could not allocate Aeron Cluster work dir", e);
        }
    }

    private static ClusteredMediaDriver clusteredMediaDriver;
    private static ClusteredServiceContainer container;

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;

    @BeforeAll
    static void launchCluster() {
        ensureDirs(PATHS);

        Archive.Context archiveContext = OmsClusterNodeBootstrap.buildArchiveContext(PATHS)
                .controlChannel(CLUSTER_ARCHIVE_CONTROL_CHANNEL);

        clusteredMediaDriver = ClusteredMediaDriver.launch(
                OmsClusterNodeBootstrap.buildMediaDriverContext(PATHS),
                archiveContext,
                OmsClusterNodeBootstrap.buildConsensusModuleContext(
                        PATHS, /* memberId = */ 0, CLUSTER_MEMBERS_SINGLE_NODE));

        container = ClusteredServiceContainer.launch(
                OmsClusterNodeBootstrap.buildServiceContainerContext(PATHS));
    }

    @AfterAll
    static void closeCluster() {
        try {
            if (container != null) {
                container.close();
                container = null;
            }
        } finally {
            if (clusteredMediaDriver != null) {
                clusteredMediaDriver.close();
                clusteredMediaDriver = null;
            }
        }
    }

    @DynamicPropertySource
    static void registerClusterClientProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.client.enabled", () -> "true");
        registry.add("oms.cluster.client.aeron-directory", PATHS::aeronDirectory);
        registry.add("oms.cluster.client.ingress-endpoints", () -> CLUSTER_INGRESS_ENDPOINTS);
    }

    @Test
    void postOrder_clusterAdmits_andPostgresInserts() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res =
                exchange(jsonRequest(accountId, "phase1-closeout-fresh"));

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getBody()).isNotNull();
        UUID orderId = UUID.fromString((String) res.getBody().get("id"));

        Long orderCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE id = ? AND account_id = ?",
                Long.class,
                orderId,
                accountId);
        assertThat(orderCount).isEqualTo(1L);

        Long outboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_outbox WHERE order_id = ?",
                Long.class,
                orderId);
        assertThat(outboxCount)
                .as("ingress transaction must still write control_outbox in Phase 1 (Postgres in same tx as cluster commit)")
                .isEqualTo(1L);

        Long domainOutboxCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?",
                Long.class,
                orderId);
        assertThat(domainOutboxCount)
                .as("ingress transaction must still write domain_event_outbox in Phase 1")
                .isEqualTo(1L);
    }

    @Test
    void postOrder_secondPostWithSameKey_isIdempotentAtClusterAndDb() {
        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> first =
                exchange(jsonRequest(accountId, "phase1-closeout-replay"));
        ResponseEntity<Map<String, Object>> second =
                exchange(jsonRequest(accountId, "phase1-closeout-replay"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isNotNull();
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().get("id"))
                .as("cluster idempotency: replay must return the original orderId so callers see a single identity")
                .isEqualTo(first.getBody().get("id"));

        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = ?",
                Long.class,
                accountId,
                "phase1-closeout-replay");
        assertThat(count)
                .as("Postgres must remain a single row even when the cluster replays the admission")
                .isEqualTo(1L);
    }

    private static void ensureDirs(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        for (String dir : new String[] {
            paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
        }) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create test dir: " + dir);
            }
        }
        IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);
    }

    private ResponseEntity<Map<String, Object>> exchange(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-OMS-Internal-Key", "test-key");
        return http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                new ParameterizedTypeReference<>() {});
    }

    private String jsonRequest(UUID accountId, String key) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "10",
                  "limitPrice": "150.00",
                  "timeInForce": "DAY"
                }
                """.formatted(accountId, key);
    }
}
