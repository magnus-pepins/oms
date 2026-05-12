package com.balh.oms.projector;

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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 2 slice 2b-2 end-to-end: projector subscribes to the cluster's events recording, decodes
 * {@link com.balh.oms.cluster.OrderAdmittedEvent} fragments, idempotently writes {@code orders}
 * rows, and advances the cursor.
 *
 * <p>Boots a Spring context with {@value OmsProfiles#POSTGRES_PROJECTOR} active — order-accept
 * beans (controller, service, gRPC, FIX) are excluded — and manually wires an
 * {@link OmsClusterIngressClient} (not Spring-managed in this profile) to drive the cluster.
 *
 * <p>The JVM-wide cluster singleton from {@link AbstractPostgresIntegrationTest} provides the
 * cluster + Archive recording (slice 2b-1 wired the recording in
 * {@link com.balh.oms.OmsClusterNodeBootstrap#startEventsRecording}). The projector reads via IPC
 * Archive replay since the test JVM shares the cluster's media driver directory.
 */
@ActiveProfiles({"test", OmsProfiles.POSTGRES_PROJECTOR})
class OmsPostgresProjectorIT extends AbstractPostgresIntegrationTest {

    private static final Duration ROW_VISIBLE_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration CLUSTER_SUBMIT_TIMEOUT = Duration.ofSeconds(10);

    @DynamicPropertySource
    static void registerProjectorProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.projector.aeron-directory", AbstractPostgresIntegrationTest::testClusterAeronDirectory);
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OmsPostgresProjector projector;

    @Autowired
    AeronProjectorCursorRepository cursorRepository;

    private OmsClusterIngressClient testIngressClient;

    @BeforeEach
    void cleanOrdersProjection() {
        // Each method asserts on the row(s) it just produced via the cluster. Truncate orders so
        // bleed-through from prior cases (in this class or a sibling Spring context that shares the
        // JVM-wide cluster singleton) does not satisfy a count-based assertion. The projector's
        // cursor self-heals via clampToRecording when the recording was recreated; we do not need to
        // touch aeron_projector_cursor here.
        jdbc.update(SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
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
    void clusterAdmittedOrder_isProjectedToOrdersTable() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-" + orderId;

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
                "projector-it-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);

        AdmissionResult result = testIngressClient.submitAcceptOrder(cmd, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(result).isInstanceOf(AdmissionResult.Accepted.class);

        await()
                .atMost(ROW_VISIBLE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    Long count = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM orders WHERE id = ?",
                            Long.class,
                            orderId);
                    assertThat(count).isEqualTo(1L);
                });

        // Validate the projector applied the right column values from the event.
        BigDecimal quantity = jdbc.queryForObject(
                "SELECT quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(quantity).isEqualByComparingTo("10");
        BigDecimal limitPrice = jdbc.queryForObject(
                "SELECT limit_price FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(limitPrice).isEqualByComparingTo("150");
        String side = jdbc.queryForObject(
                "SELECT side::text FROM orders WHERE id = ?", String.class, orderId);
        assertThat(side).isEqualTo("BUY");
        String tif = jdbc.queryForObject(
                "SELECT time_in_force FROM orders WHERE id = ?", String.class, orderId);
        assertThat(tif).isEqualTo("DAY");
        Integer shardId = jdbc.queryForObject(
                "SELECT shard_id FROM orders WHERE id = ?", Integer.class, orderId);
        assertThat(shardId).isZero();

        // Cursor advanced past zero (the recording carries cluster-internal frames so position is
        // non-trivial; we only assert it moved forward).
        Long cursorPos = cursorRepository
                .findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                .stream()
                .boxed()
                .findFirst()
                .orElse(null);
        assertThat(cursorPos).as("projector cursor advanced after applying event").isNotNull().isPositive();
        assertThat(projector.lastAppliedPosition()).isEqualTo(cursorPos);
    }

    @Test
    void duplicateClusterAdmission_doesNotInsertSecondRow() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-dup-" + orderId;

        AcceptOrderCommand fresh = sample(orderId, accountId, idemKey);
        AdmissionResult res1 = testIngressClient.submitAcceptOrder(fresh, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(((AdmissionResult.Accepted) res1).event().duplicate()).isFalse();

        await()
                .atMost(ROW_VISIBLE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    Long c = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = ?",
                            Long.class,
                            accountId,
                            idemKey);
                    assertThat(c).isEqualTo(1L);
                });

        // Same idempotency key, different orderId — cluster idempotency rejects (returns first orderId).
        AcceptOrderCommand replay = sample(UUID.randomUUID(), accountId, idemKey);
        AdmissionResult res2 = testIngressClient.submitAcceptOrder(replay, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(((AdmissionResult.Accepted) res2).event().duplicate()).isTrue();
        assertThat(((AdmissionResult.Accepted) res2).event().orderId())
                .as("cluster echoes the original orderId, not the duplicate caller's id")
                .isEqualTo(orderId);

        // Pause briefly to allow projector poll cycles to complete; the duplicate must NOT have produced
        // a second OrderAdmittedEvent on the side publication, so the orders table stays at 1 row.
        Thread.sleep(300L);

        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM orders WHERE account_id = ? AND client_idempotency_key = ?",
                Long.class,
                accountId,
                idemKey);
        assertThat(count).isEqualTo(1L);
    }

    private AcceptOrderCommand sample(UUID orderId, UUID accountId, String idemKey) {
        return new AcceptOrderCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                instantToNanos(),
                10_000_000_000L,
                150_000_000L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId.toString(),
                idemKey,
                "projector-it-hash",
                "AAPL",
                null);
    }

    private static long instantToNanos() {
        java.time.Instant now = java.time.Instant.now();
        return Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano();
    }
}
