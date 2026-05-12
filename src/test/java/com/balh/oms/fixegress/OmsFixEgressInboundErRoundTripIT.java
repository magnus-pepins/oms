package com.balh.oms.fixegress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fix.FixOutboundSessionSend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
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
 * Phase 3 slice 3d end-to-end: cluster admits an order → {@link OmsFixEgressService} replays
 * the events recording → ships {@code NewOrderSingle} to {@link EgressBrokerFillingAcceptorApplication}
 * → acceptor synthesises a {@code PARTIAL_FILL ExecutionReport} reply → OMS initiator receives
 * the ER → {@code OmsFixApplication.fromApp} routes through {@code FixInboundClusterSink} →
 * {@link OmsClusterIngressClient#submitApplyExecutionReport} offers an
 * {@code ApplyExecutionReportCommand} → cluster's
 * {@code OmsAdmissionClusteredService.applyExecutionReport} mutates state to
 * {@code STATUS_PARTIALLY_FILLED}, version 0 → 1.
 *
 * <p><strong>Verification path.</strong> The cluster's per-session egress for
 * {@code AcceptOrderCommand} carries the current {@code AdmittedOrder.version} — including on
 * the idempotency re-hit branch. Re-submitting the original {@code AcceptOrderCommand} with the
 * same {@code (accountId, clientIdempotencyKey)} after the ER round-trip therefore returns
 * {@code Accepted{duplicate=true, version=1}} iff the ER actually landed and bumped the cluster
 * state. We use this as the round-trip oracle so the IT does not need a second Aeron Archive
 * replay subscription to peek at the events stream.
 *
 * <p><strong>Cross-test isolation.</strong> Same shape as {@link OmsFixEgressBrokerIT}: per-test
 * UUIDs and an Awaitility loop on {@link OmsFixEgressService#lastAppliedPosition} so the egress's
 * pre-test backlog (events from earlier tests in the JVM) clears before assertions begin.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_EGRESS})
@Import(OmsFixEgressInboundErRoundTripItBeans.class)
class OmsFixEgressInboundErRoundTripIT extends AbstractPostgresIntegrationTest {

    private static final Duration FIX_LOGON_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration NOS_DELIVERY_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration ER_APPLY_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration CLUSTER_SUBMIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration EGRESS_BASELINE_TIMEOUT = Duration.ofSeconds(60);
    private static final long EGRESS_BASELINE_STABLE_MS = 500L;
    private static final long EGRESS_BASELINE_POLL_MS = 50L;

    private static final Path INITIATOR_FILE_STORE;

    static {
        try {
            INITIATOR_FILE_STORE = Files.createTempDirectory("oms-fix-egress-roundtrip-it-ini");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void registerRoundTripProperties(DynamicPropertyRegistry registry) {
        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        registry.add(
                "oms.cluster.fix-egress.aeron-directory",
                AbstractPostgresIntegrationTest::testClusterAeronDirectory);
        registry.add("oms.cluster.fix-egress.session-not-ready-park-nanos", () -> "5000000");

        // Order-accept transports off; cluster client ON (slice 3d invariant).
        registry.add("oms.grpc.enabled", () -> "false");
        registry.add("oms.cluster.client.enabled", () -> "true");

        // FIX initiator on, pointed at the embedded filling acceptor.
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        registerLoopbackPort(registry);
        registry.add("oms.fix.file-store-path", INITIATOR_FILE_STORE.toAbsolutePath()::toString);
        // Distinct CompIDs from the slice-3b-2 IT (INITIATOR / ACCEPTOR) so QuickFIX/J's JVM-global
        // session registry does not collide when both ITs run in the same JVM. Same shape as the
        // FixRoundTrip family of legacy ITs that pick per-IT CompIDs for the same reason.
        registry.add("oms.fix.sender-comp-id", () -> "INITIATOR_RT");
        registry.add("oms.fix.target-comp-id", () -> "ACCEPTOR_RT");
        registry.add("oms.fix.outbound-poll-interval-ms", () -> "25");
    }

    private static void registerLoopbackPort(DynamicPropertyRegistry registry) {
        final int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to allocate loopback port for round-trip IT", e);
        }
        registry.add("oms.fix.socket-connect-port", () -> String.valueOf(port));
    }

    @Autowired
    OmsFixEgressService egress;

    @Autowired
    FixOutboundSessionSend fixOutboundSessionSend;

    private OmsClusterIngressClient testIngressClient;

    @BeforeEach
    void resetTestState() throws InterruptedException {
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
        EgressBrokerFillingAcceptorApplication.resetItHooks();
    }

    @BeforeEach
    void connectIngressClient() {
        com.balh.oms.config.OmsConfig cfg = new com.balh.oms.config.OmsConfig();
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
    void admittedOrder_partialFillFromBroker_lifesClusterStateToVersion1() throws Exception {
        await()
                .atMost(FIX_LOGON_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() ->
                        assertThat(fixOutboundSessionSend.hasActiveSession())
                                .as("FIX session logged on between embedded filling acceptor and OMS initiator")
                                .isTrue());

        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        String idempotencyKey = "egress-roundtrip-it-" + orderId;
        AcceptOrderCommand cmd = buildCommand(orderId, accountId, idempotencyKey, "AAPL");

        AdmissionResult initial = testIngressClient.submitAcceptOrder(cmd, CLUSTER_SUBMIT_TIMEOUT);
        assertThat(initial).isInstanceOf(AdmissionResult.Accepted.class);
        AdmissionResult.Accepted accepted = (AdmissionResult.Accepted) initial;
        assertThat(accepted.event().version())
                .as("first AcceptOrderCommand response carries version 0")
                .isEqualTo(0);
        assertThat(accepted.event().duplicate())
                .as("first submission is not a duplicate re-hit")
                .isFalse();

        await()
                .atMost(NOS_DELIVERY_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() ->
                        assertThat(countClOrdId(orderId))
                                .as("orderId NOS delivered exactly once after cluster admission")
                                .isEqualTo(1));

        AcceptOrderCommand resubmit = buildCommand(orderId, accountId, idempotencyKey, "AAPL");
        await()
                .atMost(ER_APPLY_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    AdmissionResult resubmitResult =
                            testIngressClient.submitAcceptOrder(resubmit, CLUSTER_SUBMIT_TIMEOUT);
                    assertThat(resubmitResult).isInstanceOf(AdmissionResult.Accepted.class);
                    AdmissionResult.Accepted re = (AdmissionResult.Accepted) resubmitResult;
                    assertThat(re.event().duplicate())
                            .as("re-submission with same (accountId, idempotencyKey) is the dedupe path")
                            .isTrue();
                    assertThat(re.event().version())
                            .as("post-PARTIAL_FILL cluster state: AdmittedOrder.version bumped 0 -> 1")
                            .isEqualTo(1);
                });
    }

    private static long countClOrdId(UUID id) {
        return EgressBrokerFillingAcceptorApplication.CL_ORD_IDS_RECEIVED.stream()
                .filter(id::equals)
                .count();
    }

    private AcceptOrderCommand buildCommand(UUID orderId, UUID accountId, String idempotencyKey, String symbol) {
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
                idempotencyKey,
                "egress-roundtrip-it-hash",
                symbol,
                /* ledgerBalanceIdOrNull = */ null);
    }

    private static long instantToNanos() {
        java.time.Instant now = java.time.Instant.now();
        return Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano();
    }
}
