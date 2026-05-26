package com.balh.oms.fixin;

import com.balh.oms.cluster.ApplyVenueResolutionCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.VenueResolutionAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.settlement.PredictionMarketResolutionService;
import com.balh.oms.fixin.it.FixInClientCollectorApplication;
import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import com.balh.oms.venue.EmbeddedVenueStack;
import com.balh.oms.venueegress.OmsVenueEgressService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;
import quickfix.field.ExecType;
import quickfix.field.OrdStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 0 end-to-end: FIX-in {@code 35=D} → OMS cluster admit → {@code oms-venue-egress} gRPC →
 * embedded {@code venue-cluster-node} CLOB → cluster ER apply → FIX-in client {@code 35=8}.
 */
@ResourceLock("oms-venue-wire-it")
@ActiveProfiles({"test", OmsProfiles.FIX_INGRESS, OmsProfiles.VENUE_EGRESS})
@Import(FixInToVenueRoundTripItBeans.class)
@Sql(
        scripts = "/db/fix-in-uat-seed.sql",
        statements = {
            "DELETE FROM oms_venue_egress_cursor WHERE egress_id = 'oms-venue-egress-default'",
            "DELETE FROM oms_fix_in_return_cursor WHERE egress_id = 'oms-fix-in-return-default'"
        },
        executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class FixInToVenueRoundTripIT extends FixInWireItAcceptorSupport {

    private static final Duration LOGON_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration FLOW_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration EGRESS_BASELINE_TIMEOUT = Duration.ofSeconds(15);
    private static final long EGRESS_BASELINE_STABLE_MS = 500L;
    private static final long EGRESS_BASELINE_POLL_MS = 50L;
    private static final String PREDMKT_SYMBOL = "PREDMKT-TEST-1";

    private static final Path FIX_IN_ACCEPTOR_STORE;
    private static final int FIX_IN_ACCEPT_PORT;
    private static EmbeddedVenueStack embeddedVenue;

    static {
        try {
            FIX_IN_ACCEPTOR_STORE = Files.createTempDirectory("oms-fix-in-venue-rt-acc-store");
            FIX_IN_ACCEPT_PORT = allocatePort();
            embeddedVenue = EmbeddedVenueStack.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        String aeronDir = testClusterAeronDirectory();
        registry.add("oms.grpc.enabled", () -> "false");
        registry.add("oms.cluster.client.enabled", () -> "true");
        registry.add("oms.cluster.client.aeron-directory", () -> aeronDir);

        registry.add("oms.fix-in.enabled", () -> "true");
        registry.add("oms.fix-in.auto-start", () -> "false");
        registry.add("oms.fix-in.accept-port", () -> String.valueOf(FIX_IN_ACCEPT_PORT));
        registry.add("oms.fix-in.bind-host", () -> "127.0.0.1");
        registry.add("oms.fix-in.file-store-path", FIX_IN_ACCEPTOR_STORE.toAbsolutePath()::toString);
        registry.add("oms.fix-in.session-store-type", () -> "jdbc");
        registry.add("oms.fix-in.return-publisher.enabled", () -> "true");
        registry.add("oms.fix-in.return-publisher.aeron-directory", () -> aeronDir);
        // Differs from oms.cluster.venue-egress.replay-stream-id (4324): both replay loops run in
        // this single JVM and must not share an Aeron archive replay (channel, streamId) pair.
        registry.add("oms.fix-in.return-publisher.replay-stream-id", () -> "4325");

        registry.add("oms.cluster.venue-egress.enabled", () -> "true");
        registry.add("oms.cluster.venue-egress.aeron-directory", () -> aeronDir);
        registry.add("oms.routing.backend", () -> "internal-venue");
        registry.add("oms.venue.grpc-host", () -> "127.0.0.1");
        registry.add("oms.venue.grpc-port", () -> String.valueOf(embeddedVenue.grpcPort()));
    }

    @Autowired OmsVenueEgressService egress;
    @Autowired FixInClientEmbeddedInitiator fixInClient;
    @Autowired JdbcTemplate jdbc;
    @Autowired OmsClusterIngressClient clusterIngressClient;
    @Autowired OmsConfig omsConfig;
    @Autowired PredictionMarketResolutionService predictionMarketResolutionService;

    @BeforeEach
    void startClientAndResetHooks() throws InterruptedException {
        embeddedVenue.resetMatchingState();
        if (!fixInClient.isRunning()) {
            fixInClient.start();
        }
        resetHooksAndWaitForEgressBaseline();
    }

    @AfterEach
    void stopClient() {
        fixInClient.stop();
    }

    @AfterAll
    static void stopEmbeddedVenue() {
        if (embeddedVenue != null) {
            embeddedVenue.close();
            embeddedVenue = null;
        }
    }

    void resetHooksAndWaitForEgressBaseline() throws InterruptedException {
        FixInClientCollectorApplication.reset();
        long stable = -1L;
        long lastChange = System.currentTimeMillis();
        long deadline = lastChange + EGRESS_BASELINE_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            long current = egress.lastAppliedPosition();
            long now = System.currentTimeMillis();
            if (current != stable) {
                stable = current;
                lastChange = now;
            } else if (now - lastChange >= EGRESS_BASELINE_STABLE_MS) {
                break;
            }
            Thread.sleep(EGRESS_BASELINE_POLL_MS);
        }
    }

    @Test
    void fixInClientOrder_clobCrossFillReturnsOnFixInWire() throws Exception {
        embeddedVenue.seedRestingSell(PREDMKT_SYMBOL, 10, 0.65);
        assertThat(embeddedVenue.restingOrderCount(PREDMKT_SYMBOL)).isEqualTo(1);
        long venueTradesBefore = embeddedVenue.tradeCount();

        await().atMost(LOGON_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(fixInClient.isLoggedOn()).isTrue());

        String clOrdId = "FVRT-" + UUID.randomUUID().toString().substring(0, 8);
        fixInClient.sendNewOrderSingle(clOrdId, PREDMKT_SYMBOL, 10, 0.65);

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(embeddedVenue.tradeCount()).isEqualTo(venueTradesBefore + 1L));

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> clOrdId.equals(er.clOrdId()) && er.execType() == ExecType.NEW);
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> clOrdId.equals(er.clOrdId()) && er.execType() == ExecType.FILL);
            assertThat(FixInClientCollectorApplication.RECEIVED.stream()
                            .filter(er -> clOrdId.equals(er.clOrdId()) && er.execType() == ExecType.FILL)
                            .findFirst()
                            .orElseThrow()
                            .ordStatus())
                    .isEqualTo(OrdStatus.FILLED);
        });

        Long auditCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM oms_fix_message_audit WHERE cl_ord_id = ? AND direction = 'OUTBOUND'",
                Long.class,
                clOrdId);
        assertThat(auditCount).isGreaterThanOrEqualTo(2L);
    }

    @Test
    void fixInFillThenYesResolution_enqueuesPredictionMarketLedgerOutbox() throws Exception {
        embeddedVenue.seedRestingSell(PREDMKT_SYMBOL, 10, 0.65);
        await().atMost(LOGON_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(fixInClient.isLoggedOn()).isTrue());

        String clOrdId = "FVRES-" + UUID.randomUUID().toString().substring(0, 8);
        fixInClient.sendNewOrderSingle(clOrdId, PREDMKT_SYMBOL, 10, 0.65);

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(FixInClientCollectorApplication.RECEIVED.stream()
                                .anyMatch(er -> clOrdId.equals(er.clOrdId()) && er.execType() == ExecType.FILL))
                        .isTrue());

        String evidenceHash = "it-hash-" + UUID.randomUUID();
        embeddedVenue.resolveContractYes(PREDMKT_SYMBOL, evidenceHash);

        ApplyVenueResolutionCommand resolution =
                new ApplyVenueResolutionCommand(
                        System.nanoTime(),
                        PREDMKT_SYMBOL,
                        OmsClusterWireFormat.OUTCOME_YES,
                        "it-oracle",
                        System.currentTimeMillis(),
                        evidenceHash,
                        omsConfig.getVenue().getVenueId());
        clusterIngressClient.submitApplyVenueResolution(resolution, Duration.ofSeconds(10));

        UUID accountId = UUID.fromString("a0000001-0000-4000-8000-000000000002");
        UUID custodyId = UUID.fromString(omsConfig.getSettlement().getDefaultCustodyAccountId());
        jdbc.update(
                """
                        INSERT INTO positions (
                            account_id, instrument_symbol, custody_account_id,
                            quantity_total, quantity_pending_buy_settle, updated_at
                        ) VALUES (?, ?, ?, 10, 10, NOW())
                        ON CONFLICT (account_id, instrument_symbol, custody_account_id) DO UPDATE SET
                            quantity_total = 10,
                            quantity_pending_buy_settle = 10,
                            updated_at = NOW()
                        """,
                accountId,
                PREDMKT_SYMBOL,
                custodyId);

        predictionMarketResolutionService.apply(
                new VenueResolutionAppliedEvent(
                        PREDMKT_SYMBOL,
                        OmsClusterWireFormat.OUTCOME_YES,
                        resolution.resolutionSource(),
                        resolution.resolutionTimestampMillis(),
                        evidenceHash,
                        resolution.venueId(),
                        System.currentTimeMillis(),
                        1));

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            Integer resolutionRows =
                    jdbc.queryForObject(
                            "SELECT COUNT(*) FROM venue_contract_resolution WHERE contract_symbol = ? AND evidence_hash = ?",
                            Integer.class,
                            PREDMKT_SYMBOL,
                            evidenceHash);
            assertThat(resolutionRows).isEqualTo(1);
            Integer outboxRows =
                    jdbc.queryForObject(
                            "SELECT COUNT(*) FROM prediction_market_ledger_outbox o "
                                    + "JOIN venue_contract_resolution r ON r.id = o.resolution_id "
                                    + "WHERE r.evidence_hash = ?",
                            Integer.class,
                            evidenceHash);
            assertThat(outboxRows).isGreaterThanOrEqualTo(1);
        });

        String payout =
                jdbc.queryForObject(
                        """
                                SELECT o.payload_json::text
                                FROM prediction_market_ledger_outbox o
                                JOIN venue_contract_resolution r ON r.id = o.resolution_id
                                WHERE r.evidence_hash = ?
                                LIMIT 1
                                """,
                        String.class,
                        evidenceHash);
        assertThat(payout).contains("prediction_market_binary_resolution");
        assertThat(payout).contains("payoutAmount").contains("10.00");
    }

    private static int allocatePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
