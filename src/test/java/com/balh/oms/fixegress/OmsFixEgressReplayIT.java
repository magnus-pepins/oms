package com.balh.oms.fixegress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 3 slice 3b-1 end-to-end: {@link OmsFixEgressService} subscribes to the cluster's events
 * recording, decodes {@link com.balh.oms.cluster.OrderAdmittedEvent} fragments, and advances
 * {@code oms_fix_egress_cursor} as it goes — <em>without</em> sending NewOrderSingle yet (slice
 * 3b-2). This proves the replay infrastructure is correct before we hang the FIX wire side
 * effect off it.
 *
 * <p>Boots a Spring context with {@value OmsProfiles#FIX_EGRESS} active — order-accept beans
 * (controller, service, gRPC) are excluded — and manually wires an
 * {@link OmsClusterIngressClient} to drive the cluster (the cluster client is profile-mutex
 * with {@code oms-fix-egress} until slice 3d, so we instantiate it directly the same way
 * {@code OmsPostgresProjectorIT} does).
 *
 * <p>The JVM-wide cluster singleton from {@link AbstractPostgresIntegrationTest} provides the
 * cluster + Archive recording. The egress reads via IPC Archive replay since the test JVM
 * shares the cluster's media driver directory.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_EGRESS})
class OmsFixEgressReplayIT extends AbstractPostgresIntegrationTest {

    private static final Duration CURSOR_ADVANCE_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration CLUSTER_SUBMIT_TIMEOUT = Duration.ofSeconds(10);

    @DynamicPropertySource
    static void registerEgressProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        registry.add(
                "oms.cluster.fix-egress.aeron-directory",
                AbstractPostgresIntegrationTest::testClusterAeronDirectory);
        registry.add("oms.grpc.enabled", () -> "false");
        // Slice 3b-1: cluster client stays off (egress reads from Archive). Slice 3d enables it.
        registry.add("oms.cluster.client.enabled", () -> "false");
        // No QuickFIX wiring in 3b-1 — the loop only advances the cursor, no SocketInitiator runs.
        registry.add("oms.fix.auto-start", () -> "false");
        registry.add("oms.routing.backend", () -> "noop");
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OmsFixEgressService egress;

    @Autowired
    OmsFixEgressCursorRepository cursorRepository;

    private OmsClusterIngressClient testIngressClient;

    @BeforeEach
    void cleanCursor() {
        // Each method asserts on the cursor advance produced by the order it just submits; reset
        // the egress's persisted cursor row so prior cases (in this class or a sibling Spring
        // context that shares the JVM-wide cluster singleton) cannot satisfy the "advanced past
        // zero" assertion via leakage. The egress's clampToRecording self-heals when the cursor
        // is rewound below the recording's startPosition (it clamps up to startPosition again).
        jdbc.update(
                "DELETE FROM oms_fix_egress_cursor WHERE egress_id = ?",
                OmsFixEgressService.EGRESS_ID);
    }

    @BeforeEach
    void connectClient() {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory(testClusterAeronDirectory());
        cfg.getCluster().getClient().setIngressEndpoints(testClusterIngressEndpoints());
        testIngressClient = new OmsClusterIngressClient(cfg);
        testIngressClient.connect();
        assertThat(testIngressClient.isConnected()).isTrue();
    }

    @AfterEach
    void closeClient() {
        if (testIngressClient != null) {
            testIngressClient.close();
        }
    }

    @Test
    void clusterAdmittedOrder_advancesEgressCursor() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "egress-it-" + orderId;

        AcceptOrderCommand cmd = new AcceptOrderCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* clientTimestampNanos = */ instantToNanos(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 150_000_000L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId.toString(),
                idemKey,
                "egress-it-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);

        AdmissionResult result = testIngressClient.submitAcceptOrder(cmd, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(result).isInstanceOf(AdmissionResult.Accepted.class);

        await()
                .atMost(CURSOR_ADVANCE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    Long persisted = cursorRepository
                            .findLastAppliedPosition(
                                    OmsFixEgressService.EGRESS_ID,
                                    OmsClusterWireFormat.EVENTS_STREAM_ID)
                            .stream()
                            .boxed()
                            .findFirst()
                            .orElse(null);
                    assertThat(persisted)
                            .as("egress cursor advanced after applying admitted event")
                            .isNotNull()
                            .isPositive();
                    assertThat(egress.lastAppliedPosition()).isEqualTo(persisted);
                });
    }

    private static long instantToNanos() {
        java.time.Instant now = java.time.Instant.now();
        return Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano();
    }
}
