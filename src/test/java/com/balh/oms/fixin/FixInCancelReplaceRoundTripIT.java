package com.balh.oms.fixin;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fix.FixOutboundSessionSend;
import com.balh.oms.fixegress.EgressBrokerFillingAcceptorApplication;
import com.balh.oms.fixegress.OmsFixEgressService;
import com.balh.oms.fixin.it.FixInClientCollectorApplication;
import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * FIX-in {@code 35=F}/{@code 35=G} wire round trip through cluster + egress broker acceptor
 * (extends {@link FixInFullRoundTripIT} broker/acceptor harness).
 */
@ResourceLock("oms-fixin-wire-it")
@ActiveProfiles({"test", OmsProfiles.FIX_INGRESS, OmsProfiles.FIX_EGRESS})
@Import(FixInFullRoundTripItBeans.class)
@Sql(scripts = "/db/fix-in-uat-seed.sql")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FixInCancelReplaceRoundTripIT extends FixInWireItAcceptorSupport {

    private static final Duration LOGON_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration FLOW_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration EGRESS_BASELINE_TIMEOUT = Duration.ofSeconds(60);
    private static final long EGRESS_BASELINE_STABLE_MS = 500L;
    private static final long EGRESS_BASELINE_POLL_MS = 50L;

    private static final Path FIX_IN_ACCEPTOR_STORE;
    private static final Path FIX_EGRESS_INITIATOR_STORE;
    private static final int FIX_IN_ACCEPT_PORT;
    private static final int BROKER_ACCEPT_PORT;

    static {
        try {
            FIX_IN_ACCEPTOR_STORE = Files.createTempDirectory("oms-fix-in-cr-rt-acc-store");
            FIX_EGRESS_INITIATOR_STORE = Files.createTempDirectory("oms-fix-in-cr-rt-egress-ini");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        FIX_IN_ACCEPT_PORT = allocatePort();
        BROKER_ACCEPT_PORT = allocatePort();
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

        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        registry.add("oms.cluster.fix-egress.aeron-directory", () -> aeronDir);
        registry.add("oms.routing.backend", () -> "fix");
        registry.add("oms.fix.auto-start", () -> "true");
        registry.add("oms.fix.session-store-type", () -> "jdbc");
        registry.add("oms.fix.socket-connect-host", () -> "127.0.0.1");
        registry.add("oms.fix.socket-connect-port", () -> String.valueOf(BROKER_ACCEPT_PORT));
        registry.add("oms.fix.file-store-path", FIX_EGRESS_INITIATOR_STORE.toAbsolutePath()::toString);
        registry.add("oms.fix.sender-comp-id", () -> "INITIATOR_FIR");
        registry.add("oms.fix.target-comp-id", () -> "ACCEPTOR_FIR");
        registry.add("oms.fix.outbound-poll-interval-ms", () -> "25");
    }

    @Autowired OmsFixEgressService egress;
    @Autowired FixOutboundSessionSend fixOutboundSessionSend;
    @Autowired FixInClientEmbeddedInitiator fixInClient;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void startClientAndResetHooks() throws InterruptedException {
        if (!fixInClient.isRunning()) {
            fixInClient.start();
        }
        resetHooksAndWaitForEgressBaseline();
    }

    @AfterEach
    void stopClient() {
        fixInClient.stop();
    }

    void resetHooksAndWaitForEgressBaseline() throws InterruptedException {
        FixInClientCollectorApplication.reset();
        EgressBrokerFillingAcceptorApplication.resetItHooks();
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
    @Order(1)
    void fixInClientCancel_afterPartialFillReturnsCanceledOnFixInWire() {
        awaitLogonAndEgress();

        String origClOrdId = "FIR-" + UUID.randomUUID().toString().substring(0, 8);
        String cancelClOrdId = origClOrdId + "-C";
        fixInClient.sendNewOrderSingle(origClOrdId, "AAPL", 10, 100.0);

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.NEW);
        });

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(50)).untilAsserted(() ->
                assertThat(EgressBrokerFillingAcceptorApplication.NOS_RECEIVED.get()).isGreaterThanOrEqualTo(1));

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.PARTIAL_FILL);
        });

        fixInClient.sendOrderCancelRequest(origClOrdId, cancelClOrdId, "AAPL");

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(EgressBrokerFillingAcceptorApplication.CANCEL_RECEIVED.get()).isGreaterThanOrEqualTo(1);
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.CANCELED);
            assertThat(FixInClientCollectorApplication.RECEIVED.stream()
                            .filter(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.CANCELED)
                            .findFirst()
                            .orElseThrow()
                            .ordStatus())
                    .isEqualTo(OrdStatus.CANCELED);
        });

        Long auditCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM oms_fix_message_audit WHERE cl_ord_id = ? AND direction = 'INBOUND'",
                Long.class,
                cancelClOrdId);
        assertThat(auditCount).isGreaterThanOrEqualTo(1L);
    }

    @Test
    @Order(2)
    void fixInClientReplace_returnsReplacedOnFixInWire() {
        awaitLogonAndEgress();

        String origClOrdId = "FIR-" + UUID.randomUUID().toString().substring(0, 8);
        String replaceClOrdId = origClOrdId + "-R";
        fixInClient.sendNewOrderSingle(origClOrdId, "MSFT", 10, 50.0);

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.NEW);
        });

        fixInClient.sendOrderCancelReplaceRequest(origClOrdId, replaceClOrdId, "MSFT", 8, 99.0);

        await().atMost(FLOW_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(EgressBrokerFillingAcceptorApplication.REPLACE_RECEIVED.get()).isGreaterThanOrEqualTo(1);
            assertThat(FixInClientCollectorApplication.RECEIVED)
                    .anyMatch(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.REPLACED);
            assertThat(FixInClientCollectorApplication.RECEIVED.stream()
                            .filter(er -> origClOrdId.equals(er.clOrdId()) && er.execType() == ExecType.REPLACED)
                            .findFirst()
                            .orElseThrow()
                            .ordStatus())
                    .isEqualTo(OrdStatus.REPLACED);
        });
    }

    private void awaitLogonAndEgress() {
        await().atMost(LOGON_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(fixOutboundSessionSend.hasActiveSession()).isTrue();
            assertThat(fixInClient.isLoggedOn()).isTrue();
        });
    }

    private static int allocatePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
