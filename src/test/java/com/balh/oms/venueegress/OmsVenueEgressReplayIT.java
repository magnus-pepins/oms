package com.balh.oms.venueegress;

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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Phase 0 slice: {@link OmsVenueEgressService} replay loop advances {@code oms_venue_egress_cursor}. */
@ActiveProfiles({"test", OmsProfiles.VENUE_EGRESS})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OmsVenueEgressReplayIT extends AbstractPostgresIntegrationTest {

    private static final Duration CURSOR_ADVANCE_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration CLUSTER_SUBMIT_TIMEOUT = Duration.ofSeconds(10);

    @DynamicPropertySource
    static void registerEgressProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.venue-egress.enabled", () -> "true");
        registry.add(
                "oms.cluster.venue-egress.aeron-directory",
                AbstractPostgresIntegrationTest::testClusterAeronDirectory);
        registry.add("oms.grpc.enabled", () -> "false");
        registry.add("oms.cluster.client.enabled", () -> "false");
        registry.add("oms.routing.backend", () -> "noop");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired OmsVenueEgressService egress;
    @Autowired OmsVenueEgressCursorRepository cursorRepository;

    private OmsClusterIngressClient testIngressClient;

    @BeforeEach
    void cleanCursor() {
        jdbc.update(
                "DELETE FROM oms_venue_egress_cursor WHERE egress_id = ?",
                OmsVenueEgressService.EGRESS_ID);
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
    void clusterAdmittedOrder_advancesVenueEgressCursor() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "venue-egress-it-" + orderId;

        AcceptOrderCommand cmd =
                new AcceptOrderCommand(
                        testIngressClient.nextCorrelationId(),
                        orderId,
                        instantToNanos(),
                        10_000_000_000L,
                        650_000L,
                        0,
                        AcceptOrderCommand.SIDE_BUY,
                        AcceptOrderCommand.TIF_DAY,
                        accountId.toString(),
                        idemKey,
                        "venue-egress-it-hash",
                        "PREDMKT-TEST-1",
                        null);

        AdmissionResult result = testIngressClient.submitAcceptOrder(cmd, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(result).isInstanceOf(AdmissionResult.Accepted.class);

        await()
                .atMost(CURSOR_ADVANCE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    Long persisted =
                            cursorRepository
                                    .findLastAppliedPosition(
                                            OmsVenueEgressService.EGRESS_ID,
                                            OmsClusterWireFormat.EVENTS_STREAM_ID)
                                    .stream()
                                    .boxed()
                                    .findFirst()
                                    .orElse(null);
                    assertThat(persisted)
                            .as("venue egress cursor advanced after applying admitted event")
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
