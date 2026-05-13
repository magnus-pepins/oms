package com.balh.oms.fixegress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fix.FixNewOrderSingleBuilder;
import com.balh.oms.fix.FixOutboundSessionSend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

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
 * Phase 3 slice 3b-2 end-to-end: cluster admits an order → {@link OmsFixEgressService} replays
 * the events recording → builds {@code NewOrderSingle} via
 * {@link FixNewOrderSingleBuilder#build(com.balh.oms.cluster.OrderAdmittedEvent)} → ships it
 * through QuickFIX/J's {@code Session.sendToTarget} to the embedded loopback acceptor → cursor
 * advances after the send.
 *
 * <p>Two scenarios, both keyed by {@code orderId} (UUID) so accumulated cluster state from
 * prior test classes in the same JVM cannot pollute the assertions:
 *
 * <ol>
 *   <li><strong>Outbound NOS happy path.</strong> Submit one cluster command with a fresh
 *       {@code orderId1}, assert <em>exactly one</em> NOS with that {@code ClOrdID} lands at the
 *       broker mock.</li>
 *   <li><strong>Cursor-based dedupe / restart-from-cursor.</strong> After scenario 1 lands, stop
 *       the egress {@link OmsFixEgressService#close()} → start a fresh
 *       {@link OmsFixEgressService} with the same dependencies and call its package-private
 *       {@code init()} → wait for the replay loop to attach to the recording from the persisted
 *       cursor → assert {@code orderId1} <em>still</em> appears exactly once at the acceptor (no
 *       duplicate). Submit cluster command 2 with a fresh {@code orderId2}, assert exactly one
 *       NOS with that ClOrdID lands.</li>
 * </ol>
 *
 * <p>The IT verifies the option-1 trade-off documented on {@link OmsFixEgressService}: a clean
 * shutdown between sends produces zero duplicate NOS at the broker. The crash-mid-window failure
 * mode (where the JVM dies between {@code Session.sendToTarget} and the cursor SQL UPDATE) is
 * <em>not</em> exercised here — that is option 1's acknowledged at-least-once-at-broker
 * behaviour, recovered by broker-side {@code DupClOrdID} rejection per FIX 4.4 spec.
 *
 * <p><strong>Cross-test isolation.</strong> The JVM-wide cluster events recording from
 * {@link AbstractPostgresIntegrationTest}'s {@code TestAeronClusterSingleton} accumulates
 * {@code OrderAdmittedEvent} fragments across every test that submits an order in this JVM
 * (this IT, {@code OmsFixEgressReplayIT}, {@code OrderIngressClusterIntegrationTest}, …). The
 * autowired egress bean reads its persisted cursor at startup and may decide to rewind to the
 * recording's start (when the cursor is above the live recording position; e.g. a previous
 * Gradle run wrote a cursor that this run's recording cannot satisfy because the cluster started
 * fresh). Either way, the egress will replay <em>some</em> backlog of pre-existing events at
 * boot. The IT pins assertions to per-{@code orderId} counts (UUIDs are unique per test) and
 * waits for the egress's {@code lastAppliedPosition()} to stabilise before running each
 * scenario, so pre-existing replay does not race with the test's commands.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_EGRESS})
@Import(OmsFixEgressBrokerItBeans.class)
// See OmsFixEgressInboundErRoundTripIT for the same rationale: this profile spins up a long-lived
// OmsFixEgressService replay loop and an embedded acceptor + initiator pair. Slice 3e turns the
// projector into an ExecutionAppliedEvent consumer, so a still-cached egress context routing a
// sibling test's order out over FIX would flip that order's projected status mid-test. Mark dirty.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OmsFixEgressBrokerIT extends AbstractPostgresIntegrationTest {

    private static final Duration FIX_LOGON_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration NOS_DELIVERY_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration NO_DUPLICATE_QUIET_PERIOD = Duration.ofMillis(750);
    private static final Duration CLUSTER_SUBMIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration EGRESS_BASELINE_TIMEOUT = Duration.ofSeconds(60);
    private static final long EGRESS_BASELINE_STABLE_MS = 500L;
    private static final long EGRESS_BASELINE_POLL_MS = 50L;

    private static final Path INITIATOR_FILE_STORE;

    static {
        try {
            INITIATOR_FILE_STORE = Files.createTempDirectory("oms-fix-egress-it-ini");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void registerEgressBrokerItProperties(DynamicPropertyRegistry registry) {
        // Egress profile + replay wiring.
        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        registry.add(
                "oms.cluster.fix-egress.aeron-directory",
                AbstractPostgresIntegrationTest::testClusterAeronDirectory);
        // Tighten the session-not-ready park so the test does not waste tens of milliseconds if
        // the FIX session blips during a poll cycle.
        registry.add("oms.cluster.fix-egress.session-not-ready-park-nanos", () -> "5000000");

        // Order-accept transports off (egress JVM does not run them).
        registry.add("oms.grpc.enabled", () -> "false");
        // Slice 3d: oms-fix-egress requires a cluster client (FixInboundClusterSink offers
        // ApplyExecutionReportCommand on inbound venue ER). The parent
        // AbstractPostgresIntegrationTest wires cluster.client.{aeron-directory,ingress-endpoints}
        // to the JVM-wide TestAeronClusterSingleton, so flipping enabled=true here picks those up.
        registry.add("oms.cluster.client.enabled", () -> "true");

        // FIX initiator on, pointed at the embedded acceptor on a per-context loopback port.
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        registerLoopbackPort(registry);
        registry.add("oms.fix.file-store-path", INITIATOR_FILE_STORE.toAbsolutePath()::toString);
        registry.add("oms.fix.sender-comp-id", () -> "INITIATOR");
        registry.add("oms.fix.target-comp-id", () -> "ACCEPTOR");
        registry.add("oms.fix.outbound-poll-interval-ms", () -> "25");
    }

    /**
     * Allocates an ephemeral loopback port for the embedded acceptor + initiator pair, the same
     * way {@code FixRoundTripDynamicProperties} does for the legacy round-trip ITs. Per-context
     * port allocation prevents collisions when Spring's test context cache keeps multiple FIX
     * contexts alive simultaneously.
     */
    private static void registerLoopbackPort(DynamicPropertyRegistry registry) {
        final int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to allocate loopback port for egress IT", e);
        }
        registry.add("oms.fix.socket-connect-port", () -> String.valueOf(port));
    }

    @Autowired
    OmsConfig omsConfig;

    @Autowired
    OmsFixEgressService egress;

    @Autowired
    OmsFixEgressCursorRepository cursorRepository;

    @Autowired
    FixNewOrderSingleBuilder newOrderSingleBuilder;

    @Autowired
    FixOutboundSessionSend fixOutboundSessionSend;

    @Autowired
    io.micrometer.core.instrument.MeterRegistry meterRegistry;

    private OmsClusterIngressClient testIngressClient;

    @BeforeEach
    void resetTestState() throws InterruptedException {
        // Wait for the autowired egress to drain whatever backlog it inherited from prior tests
        // in this JVM. The autowired egress bean started during Spring context creation and may
        // have rewound (clampToRecording -> startPosition) if the persisted cursor was above the
        // live recording position; in that case it replays accumulated OrderAdmittedEvents from
        // every test that has run in this JVM. We wait until lastAppliedPosition has been stable
        // for {@link #EGRESS_BASELINE_STABLE_MS}, which means the egress has no more fragments
        // to apply. THEN reset the acceptor's NOS counters so per-orderId assertions in the test
        // body see only NOS produced by *this* test's submissions.
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
        EgressBrokerCountingAcceptorApplication.resetItHooks();
    }

    @BeforeEach
    void connectIngressClient() {
        OmsConfig cfg = new OmsConfig();
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setAeronDirectory(testClusterAeronDirectory());
        cfg.getCluster().getClient().setIngressEndpoints(testClusterIngressEndpoints());
        testIngressClient = new OmsClusterIngressClient(cfg);
        testIngressClient.connect();
        assertThat(testIngressClient.isConnected()).isTrue();
    }

    @AfterEach
    void closeIngressClient() {
        if (testIngressClient != null) {
            testIngressClient.close();
        }
    }

    @Test
    void admittedOrder_sendsExactlyOneNos_andRestartFromCursorDoesNotDuplicate() throws Exception {
        // 1. Wait for the embedded acceptor + initiator to complete logon. The egress's send
        //    path parks on session-not-ready until this completes; we observe via the bean.
        await()
                .atMost(FIX_LOGON_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() ->
                        assertThat(fixOutboundSessionSend.hasActiveSession())
                                .as("FIX session logged on between embedded acceptor and OMS initiator")
                                .isTrue());

        // 2. Submit cluster command #1; assert exactly one NOS with this orderId's ClOrdID lands
        //    at the acceptor. UUID-keyed counters survive cross-test cluster state because the
        //    UUID was generated in this scenario and cannot collide with any prior test's
        //    accumulated events.
        UUID orderId1 = UUID.randomUUID();
        UUID accountId1 = UUID.randomUUID();
        AcceptOrderCommand cmd1 = buildCommand(orderId1, accountId1, "AAPL");
        AdmissionResult r1 = testIngressClient.submitAcceptOrder(cmd1, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(r1).isInstanceOf(AdmissionResult.Accepted.class);

        await()
                .atMost(NOS_DELIVERY_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() ->
                        assertThat(countClOrdId(orderId1))
                                .as("orderId1 NOS delivered exactly once after cluster admission")
                                .isEqualTo(1));

        long cursorAfterFirstSend = cursorRepository
                .findLastAppliedPosition(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                .orElse(0L);
        assertThat(cursorAfterFirstSend)
                .as("cursor advanced after first NOS send")
                .isPositive();

        // 3. Stop the autowired egress service. close() is package-private; the test class lives
        //    in the same package so no reflection is needed. The replay thread is interrupted
        //    and joined before close() returns.
        egress.close();

        // 4. Construct a fresh egress instance with the same Spring-managed dependencies and call
        //    init() — this simulates JVM restart of the egress role without bouncing the entire
        //    Spring context (the FIX session + AbstractPostgresIntegrationTest's cluster
        //    singleton stay alive). The new instance reads the cursor row that scenario 2 wrote
        //    and resumes replay from cursorAfterFirstSend.
        OmsFixEgressService restarted =
                new OmsFixEgressService(
                        omsConfig,
                        cursorRepository,
                        meterRegistry,
                        newOrderSingleBuilder,
                        fixOutboundSessionSend);
        restarted.init();
        try {
            // Quiet period: let the restarted egress open replay subscription and confirm no
            // duplicate NOS for orderId1 arrives. If cursor-based dedupe were broken, the
            // restarted egress would replay fragment-with-orderId1 and send a duplicate NOS for
            // it. We do not submit anything during this wait, so any orderId1 NOS observed
            // would have to come from re-applying the pre-restart fragment.
            Thread.sleep(NO_DUPLICATE_QUIET_PERIOD.toMillis() * 2);
            assertThat(countClOrdId(orderId1))
                    .as("restart-from-cursor must not redeliver orderId1 fragment — option 1 cursor dedupe")
                    .isEqualTo(1);

            // 5. Submit cluster command #2 with a fresh orderId; the restarted egress picks it up
            //    from the cursor and sends a fresh NOS. orderId2 should appear exactly once at
            //    the acceptor; orderId1 must STILL be exactly once (no late duplicate).
            UUID orderId2 = UUID.randomUUID();
            UUID accountId2 = UUID.randomUUID();
            AcceptOrderCommand cmd2 = buildCommand(orderId2, accountId2, "MSFT");
            AdmissionResult r2 = testIngressClient.submitAcceptOrder(cmd2, CLUSTER_SUBMIT_TIMEOUT);
            assertThat(r2).isInstanceOf(AdmissionResult.Accepted.class);

            await()
                    .atMost(NOS_DELIVERY_TIMEOUT)
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> {
                        assertThat(countClOrdId(orderId2))
                                .as("orderId2 NOS delivered exactly once by restarted egress")
                                .isEqualTo(1);
                        assertThat(countClOrdId(orderId1))
                                .as("orderId1 still exactly once after orderId2 lands — no late duplicate")
                                .isEqualTo(1);
                    });

            long cursorAfterSecondSend = cursorRepository
                    .findLastAppliedPosition(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                    .orElse(0L);
            assertThat(cursorAfterSecondSend)
                    .as("cursor advanced past first send after restarted egress sends second NOS")
                    .isGreaterThan(cursorAfterFirstSend);
        } finally {
            restarted.close();
        }
    }

    private static long countClOrdId(UUID id) {
        return EgressBrokerCountingAcceptorApplication.CL_ORD_IDS_RECEIVED.stream()
                .filter(id::equals)
                .count();
    }

    private AcceptOrderCommand buildCommand(UUID orderId, UUID accountId, String symbol) {
        return new AcceptOrderCommand(
                testIngressClient.nextCorrelationId(),
                orderId,
                /* clientTimestampNanos = */ instantToNanos(),
                /* quantityScaled = */ 10L * AcceptOrderCommand.QUANTITY_SCALE,
                /* limitPriceScaledOrZero = */ 5L * AcceptOrderCommand.PRICE_SCALE,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId.toString(),
                "egress-broker-it-" + orderId,
                "egress-broker-it-hash",
                symbol,
                /* ledgerBalanceIdOrNull = */ null);
    }

    private static long instantToNanos() {
        java.time.Instant now = java.time.Instant.now();
        return Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano();
    }
}
