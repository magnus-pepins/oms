package com.balh.oms.projector;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
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
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 2 slice 2d end-to-end: projector subscribes to the cluster's events recording, decodes
 * {@link com.balh.oms.cluster.OrderAdmittedEvent} fragments, and per event writes the {@code orders}
 * row + runs control admission ({@code control_decisions} + {@code domain_event_outbox}) + advances
 * the cursor — all inside a single Postgres transaction.
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

        // Slice 2d: control admission ran inside the projector transaction. The orders row
        // advanced from NEW (version 0, fresh insert) to WORKING (version 1, CAS) and a
        // control_decisions PASS row + domain_event_outbox OrderWorking envelope were written.
        await()
                .atMost(ROW_VISIBLE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    String status = jdbc.queryForObject(
                            "SELECT status::text FROM orders WHERE id = ?", String.class, orderId);
                    assertThat(status).isEqualTo("WORKING");
                });
        Integer version = jdbc.queryForObject(
                "SELECT version FROM orders WHERE id = ?", Integer.class, orderId);
        assertThat(version).isEqualTo(1);

        Long passDecisions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_decisions WHERE order_id = ? AND outcome = 'PASS'",
                Long.class,
                orderId);
        assertThat(passDecisions)
                .as("projector recorded a PASS control_decisions row for the admitted order")
                .isEqualTo(1L);

        Long workingEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderWorking'",
                Long.class,
                orderId);
        assertThat(workingEnvelopes)
                .as("projector enqueued an OrderWorking domain envelope for the admitted order")
                .isEqualTo(1L);

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

        // Slice 2d: idempotency on the projector side too — exactly one PASS control_decisions row
        // and exactly one OrderWorking domain_event_outbox envelope, even though the duplicate
        // submission re-routed through the cluster.
        Long passDecisions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM control_decisions WHERE order_id = ? AND outcome = 'PASS'",
                Long.class,
                orderId);
        assertThat(passDecisions).isEqualTo(1L);

        Long workingEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderWorking'",
                Long.class,
                orderId);
        assertThat(workingEnvelopes).isEqualTo(1L);
    }

    // The IT suite shares the JVM-wide cluster singleton with sibling test classes (e.g.
    // OmsFixEgressInboundErRoundTripIT, which uses senderCompId="INITIATOR_RT" with msgSeqNum=1).
    // The cluster's slice 3d (senderCompId, msgSeqNum) wire-level dedupe set lives across tests, so
    // re-using a non-empty senderCompId here would be silently dropped at the cluster's first guard.
    // We pass senderCompId="" on every ApplyExecutionReportCommand below so the cluster opts out of
    // wire dedupe (see OmsAdmissionClusteredService.applyExecutionReport empty-string branch); the
    // (orderId, venueExecRef) cluster dedupe still holds because every test minted a fresh UUID and
    // a per-orderId venueRef.

    /**
     * Slice 3e — partial fill: the cluster admits an order, then applies a TRADE execution report
     * for half the quantity. The projector observes:
     *
     * <ul>
     *   <li>one {@code executions} row with {@code exec_type='TRADE'}, the right qty/px and
     *       {@code account_id} taken from the {@link com.balh.oms.cluster.ExecutionAppliedEvent}
     *       (no Postgres lookup needed);</li>
     *   <li>{@code orders.cum_filled_quantity} bumped to the partial qty,
     *       {@code orders.status='PARTIALLY_FILLED'}, {@code orders.version=2} (slice 2d's
     *       {@code WORKING} CAS bumped to v=1; the cluster's slice-3c apply bumped to v=2);</li>
     *   <li>one {@code domain_event_outbox} row with envelope {@code type='OrderPartiallyFilled'}.</li>
     * </ul>
     */
    @Test
    void clusterPartialFill_isProjectedToExecutionsAndOrdersAndDomainOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-trade-" + orderId;

        AdmissionResult admit = testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        assertThat(admit).isInstanceOf(AdmissionResult.Accepted.class);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-ER-" + orderId;
        ApplyExecutionReportCommand trade = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 4_000_000_000L,
                /* lastPxScaled = */ 150_500_000L,
                /* venueTsNanos = */ instantToNanos(),
                /* msgSeqNum = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"TRADE\"}");
        testIngressClient.submitApplyExecutionReport(trade, CLUSTER_SUBMIT_TIMEOUT);

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                    Long.class,
                    orderId);
            assertThat(count).isEqualTo(1L);
        });

        BigDecimal lastQty = jdbc.queryForObject(
                "SELECT last_quantity FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                BigDecimal.class,
                orderId);
        assertThat(lastQty).isEqualByComparingTo("4");
        BigDecimal lastPx = jdbc.queryForObject(
                "SELECT last_price FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                BigDecimal.class,
                orderId);
        assertThat(lastPx).isEqualByComparingTo("150.5");
        UUID execAccountId = jdbc.queryForObject(
                "SELECT account_id FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                UUID.class,
                orderId);
        assertThat(execAccountId).isEqualTo(accountId);
        String venueRefDb = jdbc.queryForObject(
                "SELECT venue_exec_ref FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                String.class,
                orderId);
        assertThat(venueRefDb).isEqualTo(venueRef);

        awaitOrdersStatus(orderId, "PARTIALLY_FILLED", 2);
        BigDecimal cumFilled = jdbc.queryForObject(
                "SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(cumFilled).isEqualByComparingTo("4");

        Long partialEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderPartiallyFilled'",
                Long.class,
                orderId);
        assertThat(partialEnvelopes).isEqualTo(1L);
    }

    /**
     * Slice 3e — full fill: trade for the entire admitted quantity terminates the order at
     * {@code FILLED}. The {@code OrderFilled} envelope carries the volume-weighted average price
     * computed over the {@code executions} TRADE rows (matches the legacy applier's contract).
     */
    @Test
    void clusterFullFill_terminalRowAndOrderFilledEnvelope() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-fill-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-FILL-" + orderId;
        ApplyExecutionReportCommand fill = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 10_000_000_000L,
                /* lastPxScaled = */ 150_500_000L,
                instantToNanos(),
                /* msgSeqNum = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\"}");
        testIngressClient.submitApplyExecutionReport(fill, CLUSTER_SUBMIT_TIMEOUT);

        awaitOrdersStatus(orderId, "FILLED", 2);
        BigDecimal cumFilled = jdbc.queryForObject(
                "SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(cumFilled).isEqualByComparingTo("10");

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                            + " AND envelope_json->>'type' = 'OrderFilled'",
                    Long.class,
                    orderId);
            assertThat(count).isEqualTo(1L);
        });
        // VWAP equals the single-fill price for a single-trade order.
        String avgPriceJson = jdbc.queryForObject(
                "SELECT envelope_json->'payload'->>'averageFillPrice' FROM domain_event_outbox"
                        + " WHERE order_id = ? AND envelope_json->>'type' = 'OrderFilled'",
                String.class,
                orderId);
        assertThat(new BigDecimal(avgPriceJson)).isEqualByComparingTo("150.5");
    }

    /**
     * Slice 3e — venue cancel: cluster applies a CANCEL ER, projector terminates the order at
     * {@code CANCELLED} and emits the {@code OrderCancelled} envelope.
     */
    @Test
    void clusterCancel_terminalRowAndOrderCancelledEnvelope() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-cancel-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-CANCEL-" + orderId;
        ApplyExecutionReportCommand cancel = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                instantToNanos(),
                /* msgSeqNum = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"CANCEL\"}");
        testIngressClient.submitApplyExecutionReport(cancel, CLUSTER_SUBMIT_TIMEOUT);

        awaitOrdersStatus(orderId, "CANCELLED", 2);

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                            + " AND envelope_json->>'type' = 'OrderCancelled'",
                    Long.class,
                    orderId);
            assertThat(count).isEqualTo(1L);
        });
        Long cancelExecutions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM executions WHERE order_id = ? AND exec_type = 'CANCEL'",
                Long.class,
                orderId);
        assertThat(cancelExecutions).isEqualTo(1L);
    }

    /**
     * Slice 3e — venue reject: cluster applies a VENUE_REJECT ER, projector terminates the order
     * at {@code REJECTED} with {@code terminal_reason='VENUE_REJECT'} and emits the
     * {@code OrderRejected} envelope.
     */
    @Test
    void clusterVenueReject_rejectedRowAndOrderRejectedEnvelope() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-reject-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-REJECT-" + orderId;
        ApplyExecutionReportCommand reject = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                instantToNanos(),
                /* msgSeqNum = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                /* rejectCodeOrZero = */ (byte) com.balh.oms.domain.RejectCode.VENUE_REJECT.ordinal(),
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"REJECT\"}");
        testIngressClient.submitApplyExecutionReport(reject, CLUSTER_SUBMIT_TIMEOUT);

        awaitOrdersStatus(orderId, "REJECTED", 2);
        String terminalReason = jdbc.queryForObject(
                "SELECT terminal_reason FROM orders WHERE id = ?", String.class, orderId);
        assertThat(terminalReason).isEqualTo("VENUE_REJECT");

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                            + " AND envelope_json->>'type' = 'OrderRejected'",
                    Long.class,
                    orderId);
            assertThat(count).isEqualTo(1L);
        });
        Long rejectExecutions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM executions WHERE order_id = ? AND exec_type = 'REJECT'",
                Long.class,
                orderId);
        assertThat(rejectExecutions).isEqualTo(1L);
    }

    /**
     * Slice 3e — duplicate ER: cluster's slice-3c {@code (orderId, venueExecRef)} dedupe stops the
     * second {@code ApplyExecutionReportCommand} from emitting a second {@link
     * com.balh.oms.cluster.ExecutionAppliedEvent}, so the projector never sees it. End state:
     * exactly one {@code executions} row, {@code orders.version=2}, exactly one
     * {@code OrderPartiallyFilled} envelope.
     */
    @Test
    void duplicateClusterApplyExecution_isProjectedExactlyOnce() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-dup-er-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-DUP-ER-" + orderId;
        ApplyExecutionReportCommand trade = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                4_000_000_000L,
                150_500_000L,
                instantToNanos(),
                1,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\"}");
        testIngressClient.submitApplyExecutionReport(trade, CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "PARTIALLY_FILLED", 2);

        // Same venueExecRef, distinct correlationId / msgSeqNum: cluster dedupe by
        // (orderId, venueExecRef) drops it.
        ApplyExecutionReportCommand dup = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                4_000_000_000L,
                150_500_000L,
                instantToNanos(),
                /* msgSeqNum = */ 7,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\"}");
        testIngressClient.submitApplyExecutionReport(dup, CLUSTER_SUBMIT_TIMEOUT);
        Thread.sleep(300L);

        Long executions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM executions WHERE order_id = ?",
                Long.class,
                orderId);
        assertThat(executions).isEqualTo(1L);
        Integer version = jdbc.queryForObject(
                "SELECT version FROM orders WHERE id = ?", Integer.class, orderId);
        assertThat(version).isEqualTo(2);
        Long partialEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderPartiallyFilled'",
                Long.class,
                orderId);
        assertThat(partialEnvelopes).isEqualTo(1L);
    }

    private void awaitOrdersStatus(UUID orderId, String expectedStatus, int expectedVersion) {
        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            String status = jdbc.queryForObject(
                    "SELECT status::text FROM orders WHERE id = ?", String.class, orderId);
            assertThat(status).isEqualTo(expectedStatus);
            Integer version = jdbc.queryForObject(
                    "SELECT version FROM orders WHERE id = ?", Integer.class, orderId);
            assertThat(version).isEqualTo(expectedVersion);
        });
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
        Instant now = Instant.now();
        return Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano();
    }
}
