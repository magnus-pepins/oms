package com.balh.oms.projector;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import com.balh.oms.test.SettlementBrokerDrainAssertions;
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

    /**
     * Default custody account from {@code OmsConfig.Settlement.defaultCustodyAccountId} (fixture
     * V11). The projector hands this UUID to {@link com.balh.oms.persistence.PositionsRepository}
     * for every TRADE fill, so positions-side assertions in this IT use the same id.
     */
    private static final String DEFAULT_CUSTODY_ACCOUNT_ID = "a0000001-0000-4000-8000-000000000001";

    @DynamicPropertySource
    static void registerProjectorProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.projector.aeron-directory", AbstractPostgresIntegrationTest::testClusterAeronDirectory);
        // Slice 3e-2: turn on free-riding attribution so {@link #freeRidingAttribution_secondBuyFundedByFirst}
        // exercises the projector's executions.unsettled_funded_by_exec_ids merge. The flag is a
        // no-op for the rest of the tests in this class because each test uses a fresh accountId
        // and the per-test truncate (SQL_TRUNCATE_ORDERS_AND_SETTLEMENT) clears `executions`, so
        // the prior-unsettled-BUY lookup returns empty and the merge is skipped.
        registry.add("oms.settlement.free-riding-attribution-enabled", () -> "true");
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    OmsPostgresProjector projector;

    @Autowired
    AeronProjectorCursorRepository cursorRepository;

    @Autowired
    SettlementConfirmProcessor settlementConfirmProcessor;

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

        // Slice 3e-2: market_context evidence merge — venue ER fields land in snapshot_json with
        // MarketContextVenueEvidence schemaVersion=1 (instrumentSymbol + venueExecRef + lastQuantity
        // round-tripped); the projector wires the same MarketContextVenueEvidence helper that the
        // legacy applier used, so the row shape is identical.
        Integer marketContextRows = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM market_context WHERE order_id = ?",
                Integer.class,
                orderId);
        assertThat(marketContextRows).isEqualTo(1);
        String venueRefSnapshot = jdbc.queryForObject(
                "SELECT snapshot_json->>'venueExecRef' FROM market_context WHERE order_id = ?",
                String.class,
                orderId);
        assertThat(venueRefSnapshot).isEqualTo(venueRef);
        String symbolSnapshot = jdbc.queryForObject(
                "SELECT snapshot_json->>'instrumentSymbol' FROM market_context WHERE order_id = ?",
                String.class,
                orderId);
        assertThat(symbolSnapshot).isEqualTo("AAPL");
        Integer schemaVersion = jdbc.queryForObject(
                "SELECT (snapshot_json->>'schemaVersion')::int FROM market_context WHERE order_id = ?",
                Integer.class,
                orderId);
        assertThat(schemaVersion).isEqualTo(1);

        // Slice 3e-2: positions BUY fill — quantity_total + quantity_pending_buy_settle bumped to
        // the fill qty against the default custody account; position_history TRADE_BUY_FILL row.
        BigDecimal positionTotal = jdbc.queryForObject(
                "SELECT quantity_total FROM positions WHERE account_id = ?"
                        + " AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                BigDecimal.class,
                accountId,
                UUID.fromString(DEFAULT_CUSTODY_ACCOUNT_ID));
        assertThat(positionTotal).isEqualByComparingTo("4");
        BigDecimal positionPendingBuy = jdbc.queryForObject(
                "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ?"
                        + " AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                BigDecimal.class,
                accountId,
                UUID.fromString(DEFAULT_CUSTODY_ACCOUNT_ID));
        assertThat(positionPendingBuy).isEqualByComparingTo("4");
        Long historyRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM position_history WHERE account_id = ?"
                        + " AND event_type = 'TRADE_BUY_FILL'",
                Long.class,
                accountId);
        assertThat(historyRows).isEqualTo(1L);
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

        // Slice 3e-2: legacy applier wrote a stub-only market_context row before tryInsertVenueReject
        // so orders that reject pre-trade still appear in best-ex evidence reports. The projector
        // now does the same. The "{\"stub\":true}" default comes from OmsConfig.Routing.
        Boolean stubSnapshot = jdbc.queryForObject(
                "SELECT (snapshot_json->>'stub')::boolean FROM market_context WHERE order_id = ?",
                Boolean.class,
                orderId);
        assertThat(stubSnapshot).isTrue();
    }

    /**
     * Wed-demo (modify) — venue REPLACE ACK: cluster applies a REPLACE ER (ET=5) carrying the
     * broker-authoritative new total qty and new limit price; the projector overwrites
     * {@code orders.quantity} and {@code orders.limit_price}, bumps version, emits an
     * {@code OrderReplaced} envelope, and writes a {@code REPLACE} audit row in {@code executions}.
     * cumQty stays at zero (pure replace, no fill).
     */
    @Test
    void clusterReplace_orderQuantityAndLimitPriceUpdatedAndOrderReplacedEnvelope() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-replace-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        // sample() places qty=10, limit=150. Replace to qty=15, limit=155.
        // QUANTITY_SCALE=1e9, PRICE_SCALE=1e6 (see AcceptOrderCommand).
        long newQtyScaled = 15_000_000_000L;
        long newLimitPxScaled = 155_000_000L;
        String venueRef = "VENUE-REPLACE-" + orderId;
        ApplyExecutionReportCommand replace = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = new total qty */ newQtyScaled,
                /* lastPxScaled = new limit px */ newLimitPxScaled,
                instantToNanos(),
                /* msgSeqNum = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_REPLACE,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"REPLACE\"}");
        testIngressClient.submitApplyExecutionReport(replace, CLUSTER_SUBMIT_TIMEOUT);

        // Status stays WORKING (cumQty=0 < newQty=15), version bumps to 2.
        awaitOrdersStatus(orderId, "WORKING", 2);

        BigDecimal qty = jdbc.queryForObject(
                "SELECT quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(qty).isEqualByComparingTo("15");
        BigDecimal limitPrice = jdbc.queryForObject(
                "SELECT limit_price FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(limitPrice).isEqualByComparingTo("155");
        BigDecimal cum = jdbc.queryForObject(
                "SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(cum).isEqualByComparingTo("0");

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                            + " AND envelope_json->>'type' = 'OrderReplaced'",
                    Long.class,
                    orderId);
            assertThat(count).isEqualTo(1L);
        });
        // Envelope payload must carry the broker-authoritative new qty + new limit price so the
        // BFF consumer can mirror onto customer_orders without a second OMS read.
        String envelopeJson = jdbc.queryForObject(
                "SELECT envelope_json::text FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderReplaced'",
                String.class,
                orderId);
        assertThat(envelopeJson).contains("\"newQuantity\":15");
        assertThat(envelopeJson).contains("\"newLimitPrice\":155");
        assertThat(envelopeJson).contains("\"newStatus\":\"WORKING\"");

        Long replaceExecutions = jdbc.queryForObject(
                "SELECT COUNT(*) FROM executions WHERE order_id = ? AND exec_type = 'REPLACE'",
                Long.class,
                orderId);
        assertThat(replaceExecutions).isEqualTo(1L);
    }

    /**
     * Slice 3e-2 — SELL fill: sell-side TRADE drains a pre-existing position and records the
     * pending-buy-vs-settled split on {@code executions} so the operator mark-failed unwind path
     * can reverse the fill exactly. The flow:
     *
     * <ol>
     *   <li>seed a {@code positions} row with {@code quantity_total = 10}, all in
     *       {@code quantity_pending_buy_settle} — this is what an unsettled BUY fill would have
     *       produced under the legacy applier;</li>
     *   <li>admit a SELL order for 4 units against the same account+symbol;</li>
     *   <li>fully fill it via the cluster.</li>
     * </ol>
     *
     * <p>Expected end state: {@code quantity_pending_buy_settle = 6} (4 drained from pending-buy
     * because the seeded position had no settled units yet), {@code quantity_pending_sell_settle
     * = 4} (the SELL fill is in flight to broker settlement), {@code quantity_total = 6};
     * {@code executions.sell_position_from_pending_buy = 4} on the inserted TRADE row;
     * {@code position_history.TRADE_SELL_FILL} row carrying {@code quantity_delta = -4}.
     */
    @Test
    void clusterSellFill_drainsPositionAndRecordsSplitOnExecutions() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-sell-" + orderId;
        UUID custody = UUID.fromString(DEFAULT_CUSTODY_ACCOUNT_ID);

        // Seed an unsettled BUY position the SELL fill will drain. Legacy applier produces the
        // same shape from a TRADE_BUY_FILL on a fresh symbol; we shortcut to it here so the test
        // can assert a single SELL apply without first running a BUY round-trip.
        jdbc.update(
                "INSERT INTO positions (account_id, instrument_symbol, custody_account_id,"
                        + " quantity_total, quantity_pending_buy_settle, quantity_settled,"
                        + " quantity_pending_sell_settle, updated_at)"
                        + " VALUES (?, 'AAPL', ?, 10, 10, 0, 0, NOW())",
                accountId,
                custody);

        AcceptOrderCommand sell = new AcceptOrderCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                instantToNanos(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 150_000_000L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_DAY,
                accountId.toString(),
                idemKey,
                "projector-it-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
        testIngressClient.submitAcceptOrder(sell, CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        String venueRef = "VENUE-SELL-" + orderId;
        ApplyExecutionReportCommand fill = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 4_000_000_000L,
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

        awaitOrdersStatus(orderId, "PARTIALLY_FILLED", 2);

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            BigDecimal pendingBuy = jdbc.queryForObject(
                    "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ?"
                            + " AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                    BigDecimal.class,
                    accountId,
                    custody);
            assertThat(pendingBuy).isEqualByComparingTo("6");
        });

        BigDecimal pendingSell = jdbc.queryForObject(
                "SELECT quantity_pending_sell_settle FROM positions WHERE account_id = ?"
                        + " AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                BigDecimal.class,
                accountId,
                custody);
        assertThat(pendingSell).isEqualByComparingTo("4");
        BigDecimal total = jdbc.queryForObject(
                "SELECT quantity_total FROM positions WHERE account_id = ?"
                        + " AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                BigDecimal.class,
                accountId,
                custody);
        assertThat(total).isEqualByComparingTo("6");

        BigDecimal sellFromPb = jdbc.queryForObject(
                "SELECT sell_position_from_pending_buy FROM executions"
                        + " WHERE order_id = ? AND exec_type = 'TRADE'",
                BigDecimal.class,
                orderId);
        assertThat(sellFromPb).isEqualByComparingTo("4");
        BigDecimal sellFromSettled = jdbc.queryForObject(
                "SELECT sell_position_from_settled FROM executions"
                        + " WHERE order_id = ? AND exec_type = 'TRADE'",
                BigDecimal.class,
                orderId);
        assertThat(sellFromSettled).isEqualByComparingTo("0");

        Long sellHistoryRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM position_history WHERE account_id = ?"
                        + " AND event_type = 'TRADE_SELL_FILL'",
                Long.class,
                accountId);
        assertThat(sellHistoryRows).isEqualTo(1L);
    }

    /**
     * Slice 3e-2 — free-riding attribution: when {@code oms.settlement.free-riding-attribution-enabled=true}
     * (turned on at the class level), every BUY {@code TRADE} fill appends prior unsettled BUY
     * execution ids on the same account+symbol to {@code unsettled_funded_by_exec_ids} on the
     * just-inserted row. Same algebra the legacy applier used (deleted in slice 3g-2).
     *
     * <p>Flow: admit-then-fill BUY #1, then admit-then-fill BUY #2 against the same account+symbol.
     * BUY #2's execution row should reference BUY #1's execution id in
     * {@code unsettled_funded_by_exec_ids} (the only prior unsettled trade execution for that
     * account+symbol — first BUY's settlement_status defaults to {@code executed}, which the
     * lookup treats as "still in flight").
     */
    @Test
    void freeRidingAttribution_secondBuyFundedByFirst() throws Exception {
        UUID accountId = UUID.randomUUID();

        UUID firstOrderId = UUID.randomUUID();
        testIngressClient.submitAcceptOrder(
                sample(firstOrderId, accountId, "fr-it-1-" + firstOrderId), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(firstOrderId, "WORKING", 1);
        String firstVenueRef = "VENUE-FR-1-" + firstOrderId;
        testIngressClient.submitApplyExecutionReport(
                tradeForFreeRiding(firstOrderId, firstVenueRef, /* msgSeq = */ 1),
                CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(firstOrderId, "FILLED", 2);
        Long firstExecutionId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                Long.class,
                firstOrderId);
        assertThat(firstExecutionId).isNotNull();

        UUID secondOrderId = UUID.randomUUID();
        testIngressClient.submitAcceptOrder(
                sample(secondOrderId, accountId, "fr-it-2-" + secondOrderId), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(secondOrderId, "WORKING", 1);
        String secondVenueRef = "VENUE-FR-2-" + secondOrderId;
        testIngressClient.submitApplyExecutionReport(
                tradeForFreeRiding(secondOrderId, secondVenueRef, /* msgSeq = */ 2),
                CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(secondOrderId, "FILLED", 2);

        await().atMost(ROW_VISIBLE_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Long fundingId = jdbc.queryForObject(
                    "SELECT (unsettled_funded_by_exec_ids)[1] FROM executions"
                            + " WHERE order_id = ? AND exec_type = 'TRADE'",
                    Long.class,
                    secondOrderId);
            assertThat(fundingId)
                    .as("second BUY references the first BUY's execution id in unsettled_funded_by_exec_ids")
                    .isEqualTo(firstExecutionId);
        });

        // First BUY's array stays empty (it was the prior trade; no earlier unsettled fills exist).
        Object firstFunding = jdbc.queryForObject(
                "SELECT unsettled_funded_by_exec_ids FROM executions"
                        + " WHERE order_id = ? AND exec_type = 'TRADE'",
                Object.class,
                firstOrderId);
        assertThat(firstFunding).asString().isIn("{}", "[]", "");
    }

    private ApplyExecutionReportCommand tradeForFreeRiding(UUID orderId, String venueRef, int msgSeq) {
        return new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 10_000_000_000L,
                /* lastPxScaled = */ 150_500_000L,
                instantToNanos(),
                msgSeq,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "VENUE",
                venueRef,
                "",
                "{\"kind\":\"ExecutionReport\"}");
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

    /**
     * Slice 3g-2 — multi-fill end-to-end: three TRADE applies for {@code ⅓ + ⅓ + ⅓} of the admitted
     * quantity terminate the order at {@code FILLED} with three {@code executions} rows, then the
     * settlement broker drain settles every TRADE row and clears the BUY-pending position. Replaces
     * the legacy {@code SimulatedReturnPathIntegrationTest} BUY scenario verbatim — the unique value
     * that test held was the multi-fill cumulative behaviour stitched with the broker drain
     * lifecycle, neither of which any other slice 3e/3e-2 case exercises end-to-end.
     */
    @Test
    void clusterMultiplePartialFills_terminateAsFilledAndSettleViaBrokerDrain() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idemKey = "projector-it-multi-fill-" + orderId;

        testIngressClient.submitAcceptOrder(sample(orderId, accountId, idemKey), CLUSTER_SUBMIT_TIMEOUT);
        awaitOrdersStatus(orderId, "WORKING", 1);

        // Order quantity is 10 (sample()'s scaled 10_000_000_000L = 10 with 1e9 scale). Split into
        // three TRADE fills (3 + 3 + 4) to mirror the legacy 3-way split-in-thirds shape.
        submitTrade(orderId, /* msgSeq = */ 1, /* venueRef = */ "VENUE-MF-1-" + orderId,
                /* lastQtyScaled = */ 3_000_000_000L);
        awaitOrdersStatus(orderId, "PARTIALLY_FILLED", 2);

        submitTrade(orderId, 2, "VENUE-MF-2-" + orderId, 3_000_000_000L);
        awaitOrdersStatus(orderId, "PARTIALLY_FILLED", 3);

        submitTrade(orderId, 3, "VENUE-MF-3-" + orderId, 4_000_000_000L);
        awaitOrdersStatus(orderId, "FILLED", 4);

        BigDecimal cumFilled = jdbc.queryForObject(
                "SELECT cum_filled_quantity FROM orders WHERE id = ?", BigDecimal.class, orderId);
        assertThat(cumFilled).isEqualByComparingTo("10");

        Long execRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM executions WHERE order_id = ? AND exec_type = 'TRADE'",
                Long.class,
                orderId);
        assertThat(execRows).isEqualTo(3L);

        Long partialEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderPartiallyFilled'",
                Long.class,
                orderId);
        assertThat(partialEnvelopes).isEqualTo(2L);
        Long filledEnvelopes = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event_outbox WHERE order_id = ?"
                        + " AND envelope_json->>'type' = 'OrderFilled'",
                Long.class,
                orderId);
        assertThat(filledEnvelopes).isEqualTo(1L);

        Integer historyRows = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM position_history WHERE account_id = ?"
                        + " AND event_type = 'TRADE_BUY_FILL'",
                Integer.class,
                accountId);
        assertThat(historyRows).isEqualTo(3);

        SettlementBrokerDrainAssertions.assertFullBrokerLifecycleSettles(
                jdbc, settlementConfirmProcessor, orderId, /* expectedTradeExecs = */ 3, new BigDecimal("10"));
    }

    private void submitTrade(UUID orderId, int msgSeq, String venueRef, long lastQtyScaled) throws Exception {
        ApplyExecutionReportCommand trade = new ApplyExecutionReportCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                lastQtyScaled,
                /* lastPxScaled = */ 150_000_000L,
                instantToNanos(),
                msgSeq,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                "VENUE",
                venueRef,
                /* senderCompId = */ "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"TRADE\"}");
        testIngressClient.submitApplyExecutionReport(trade, CLUSTER_SUBMIT_TIMEOUT);
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
