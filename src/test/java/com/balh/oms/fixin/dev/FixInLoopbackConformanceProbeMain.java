package com.balh.oms.fixin.dev;

import com.balh.oms.fixin.it.FixInClientCollectorApplication;
import com.balh.oms.fixin.it.FixInClientEmbeddedInitiator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.field.ExecType;
import quickfix.field.MsgType;
import quickfix.field.SessionRejectReason;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.awaitility.Awaitility;

/**
 * One-shot FIX-in conformance probe for bench/UAT (scenarios 6, 7, 8, 11). Connects to a live
 * {@code oms-fix-ingress} acceptor — stop the PM2 {@code oms-fix-in-loopback-client} first to
 * avoid CompID contention on {@code LOOPBACK_CLIENT}.
 *
 * <p>Run:
 *
 * <pre>
 *   ./gradlew fixInLoopbackConformanceProbe
 * </pre>
 */
public final class FixInLoopbackConformanceProbeMain {

    private static final Logger log = LoggerFactory.getLogger(FixInLoopbackConformanceProbeMain.class);

    private static final Duration LOGON_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration WIRE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9877;
    private static final String DEFAULT_ORDER_ENTRY_SENDER = "LOOPBACK_CLIENT";
    private static final String DEFAULT_DROP_COPY_SENDER = "LOOPBACK_DROP";
    private static final String DEFAULT_TARGET = "BALH_OMS";
    private static final String DEFAULT_INGRESS_URL = "http://127.0.0.1:8095";
    private static final String DEFAULT_ORDER_ENTRY_SESSION_ID = "00000001-0000-4000-8000-000000000001";
    private static final String DEFAULT_REPORT = "./build/fix-in-conformance-probe-report.json";
    private static final long DEFAULT_STALE_MS = 125_000L;

    public static void main(String[] args) throws Exception {
        ProbeConfig cfg = ProbeConfig.fromEnv();
        List<ScenarioResult> results = new ArrayList<>();

        Path orderEntryStore = cfg.probeStorePath("order-entry");
        Path dropCopyStore = cfg.probeStorePath("drop-copy");

        try {
            results.add(probeDuplicateClOrdId(cfg, orderEntryStore));
            results.add(probeDropCopyReject(cfg, dropCopyStore));
            results.add(probeStaleSendingTime(cfg, orderEntryStore));
            results.add(probeSequenceReset(cfg, orderEntryStore));
        } finally {
            writeReport(cfg.reportPath(), results);
        }

        long failed = results.stream().filter(r -> !r.passed()).count();
        log.info("FIX-in conformance probe finished pass={} fail={}", results.size() - failed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    /** Scenario 6 — duplicate ClOrdID is idempotent (same OrderID on both NEW acks). */
    private static ScenarioResult probeDuplicateClOrdId(ProbeConfig cfg, Path store) {
        FixInClientCollectorApplication.reset();
        FixInClientEmbeddedInitiator client = new FixInClientEmbeddedInitiator(
                cfg.connectPort(), store, cfg.orderEntrySender(), cfg.target(), new FixInClientCollectorApplication());
        String clOrdId = "CONF-DUP-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            client.start();
            awaitLogon(client);
            client.sendNewOrderSingle(clOrdId, "AAPL", 10, 100.0);
            client.sendNewOrderSingle(clOrdId, "AAPL", 10, 100.0);
            Awaitility.await().atMost(WIRE_TIMEOUT).pollInterval(Duration.ofMillis(100)).until(() -> {
                assertReceivedErCount(clOrdId, 2);
                return true;
            });
            List<FixInClientCollectorApplication.ReceivedEr> ers = ersForClOrdId(clOrdId);
            if (ers.size() < 2) {
                return fail("6_duplicate_cl_ord_id", "expected 2 ERs, got " + ers.size());
            }
            if (ers.stream().anyMatch(er -> er.execType() != ExecType.NEW)) {
                return fail("6_duplicate_cl_ord_id", "expected ExecType=NEW on both ERs");
            }
            String firstOrderId = ers.get(0).orderIdOrNull();
            String secondOrderId = ers.get(1).orderIdOrNull();
            if (firstOrderId == null || secondOrderId == null || !firstOrderId.equals(secondOrderId)) {
                return fail(
                        "6_duplicate_cl_ord_id",
                        "expected same OrderID on duplicate, got " + firstOrderId + " vs " + secondOrderId);
            }
            return pass("6_duplicate_cl_ord_id", "duplicate ClOrdID idempotent orderId=" + firstOrderId);
        } catch (Exception e) {
            return fail("6_duplicate_cl_ord_id", e.getMessage());
        } finally {
            client.stop();
        }
    }

    /** Scenario 7 — DROP_COPY session rejects order entry with BusinessMessageReject. */
    private static ScenarioResult probeDropCopyReject(ProbeConfig cfg, Path store) {
        FixInClientCollectorApplication.reset();
        FixInClientEmbeddedInitiator client = new FixInClientEmbeddedInitiator(
                cfg.connectPort(), store, cfg.dropCopySender(), cfg.target(), new FixInClientCollectorApplication());
        String clOrdId = "CONF-DC-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            client.start();
            awaitLogon(client);
            client.sendNewOrderSingle(clOrdId, "AAPL", 1, 1.0);
            Awaitility.await().atMost(WIRE_TIMEOUT).pollInterval(Duration.ofMillis(100)).until(() -> {
                assertBmrCountAtLeast(1);
                return true;
            });
            FixInClientCollectorApplication.ReceivedBmr bmr =
                    FixInClientCollectorApplication.BUSINESS_REJECTS.get(0);
            if (!MsgType.ORDER_SINGLE.equals(bmr.refMsgType())) {
                return fail("7_drop_copy_reject", "unexpected RefMsgType=" + bmr.refMsgType());
            }
            if (!bmr.text().contains("drop_copy_session_order_entry_forbidden")) {
                return fail("7_drop_copy_reject", "unexpected BMR text=" + bmr.text());
            }
            return pass("7_drop_copy_reject", "DROP_COPY rejected 35=D with BMR");
        } catch (Exception e) {
            return fail("7_drop_copy_reject", e.getMessage());
        } finally {
            client.stop();
        }
    }

    /** Scenario 11 — stale SendingTime → BusinessMessageReject message_too_old. */
    private static ScenarioResult probeStaleSendingTime(ProbeConfig cfg, Path store) {
        FixInClientCollectorApplication.reset();
        FixInClientEmbeddedInitiator client = new FixInClientEmbeddedInitiator(
                cfg.connectPort(), store, cfg.orderEntrySender(), cfg.target(), new FixInClientCollectorApplication());
        String clOrdId = "CONF-STALE-" + UUID.randomUUID().toString().substring(0, 8);
        LocalDateTime stale = LocalDateTime.now(ZoneOffset.UTC).minus(Duration.ofMillis(cfg.staleMs()));
        try {
            client.start();
            awaitLogon(client);
            int erBefore = FixInClientCollectorApplication.RECEIVED.size();
            client.sendNewOrderSingleWithSendingTime(clOrdId, "AAPL", 1, 1.0, stale);
            Awaitility.await().atMost(WIRE_TIMEOUT).pollInterval(Duration.ofMillis(100)).until(() -> {
                assertRateLimitRejectObserved();
                return true;
            });
            if (!FixInClientCollectorApplication.BUSINESS_REJECTS.isEmpty()) {
                FixInClientCollectorApplication.ReceivedBmr bmr =
                        FixInClientCollectorApplication.BUSINESS_REJECTS.get(
                                FixInClientCollectorApplication.BUSINESS_REJECTS.size() - 1);
                if (!MsgType.ORDER_SINGLE.equals(bmr.refMsgType())) {
                    return fail("11_rate_limit_stale_time", "unexpected RefMsgType=" + bmr.refMsgType());
                }
                if (!bmr.text().contains("message_too_old") && !bmr.text().contains("rate_limit_exceeded")) {
                    return fail("11_rate_limit_stale_time", "unexpected BMR text=" + bmr.text());
                }
                return pass("11_rate_limit_stale_time", "BMR " + bmr.text());
            }
            FixInClientCollectorApplication.ReceivedReject reject =
                    FixInClientCollectorApplication.SESSION_REJECTS.get(
                            FixInClientCollectorApplication.SESSION_REJECTS.size() - 1);
            if (reject.sessionRejectReason() == SessionRejectReason.SENDINGTIME_ACCURACY_PROBLEM
                    || reject.text().toLowerCase().contains("sendingtime")) {
                int erAfter = FixInClientCollectorApplication.RECEIVED.size();
                if (erAfter > erBefore) {
                    return fail("11_rate_limit_stale_time", "stale NOS should not produce ER");
                }
                return pass("11_rate_limit_stale_time", "session Reject for stale SendingTime");
            }
            return fail("11_rate_limit_stale_time", "unexpected reject " + reject.text());
        } catch (Exception e) {
            return fail("11_rate_limit_stale_time", e.getMessage());
        } finally {
            client.stop();
        }
    }

    /** Scenario 8 — audited sequence reset after logout. */
    private static ScenarioResult probeSequenceReset(ProbeConfig cfg, Path store) throws Exception {
        HttpClient http = cfg.httpClient();
        UUID sessionId = UUID.fromString(cfg.orderEntrySessionId());

        HttpResponse<String> logoutResp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(cfg.ingressUrl()
                                + "/internal/v1/fix-in/sessions/"
                                + sessionId
                                + "/logout"))
                        .header("X-OMS-Internal-Key", cfg.apiKey())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"requestedBy\":\"conformance-probe\",\"reason\":\"sequence reset probe\"}"))
                        .timeout(ADMIN_TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (logoutResp.statusCode() / 100 != 2) {
            return fail("8_sequence_reset", "logout HTTP " + logoutResp.statusCode() + " " + logoutResp.body());
        }

        Awaitility.await().atMost(ADMIN_TIMEOUT).pollInterval(Duration.ofMillis(250)).until(() -> {
            boolean loggedOn = fetchSessionLoggedOn(http, cfg, sessionId);
            return !loggedOn;
        });

        String resetBody =
                "{\"requestedBy\":\"conformance-probe\",\"approvedBy\":\"conformance-probe\",\"reason\":\"conformance probe\","
                        + "\"counterpartyReference\":\"probe\","
                        + "\"nextSenderSeq\":1,\"nextTargetSeq\":1}";
        HttpResponse<String> resetResp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(cfg.ingressUrl()
                                + "/internal/v1/fix-in/sessions/"
                                + sessionId
                                + "/sequence-reset"))
                        .header("X-OMS-Internal-Key", cfg.apiKey())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(resetBody))
                        .timeout(ADMIN_TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resetResp.statusCode() / 100 != 2) {
            return fail("8_sequence_reset", "sequence-reset HTTP " + resetResp.statusCode() + " " + resetResp.body());
        }

        HttpResponse<String> actionsResp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(cfg.ingressUrl()
                                + "/internal/v1/fix-in/admin-actions?sessionId="
                                + sessionId
                                + "&limit=10"))
                        .header("X-OMS-Internal-Key", cfg.apiKey())
                        .GET()
                        .timeout(ADMIN_TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (actionsResp.statusCode() / 100 != 2) {
            return fail("8_sequence_reset", "admin-actions HTTP " + actionsResp.statusCode());
        }
        if (!actionsResp.body().contains("SEQUENCE_RESET")) {
            return fail("8_sequence_reset", "no SEQUENCE_RESET in admin-actions response");
        }

        FixInClientEmbeddedInitiator client = new FixInClientEmbeddedInitiator(
                cfg.connectPort(), store, cfg.orderEntrySender(), cfg.target(), new FixInClientCollectorApplication());
        try {
            client.restartFresh();
            awaitLogon(client);
            return pass("8_sequence_reset", "SEQUENCE_RESET audited and client re-logon OK");
        } catch (Exception e) {
            return fail("8_sequence_reset", "re-logon after reset failed: " + e.getMessage());
        } finally {
            client.stop();
        }
    }

    private static boolean fetchSessionLoggedOn(HttpClient http, ProbeConfig cfg, UUID sessionId) throws Exception {
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(cfg.ingressUrl() + "/internal/v1/fix-in/sessions/" + sessionId))
                        .header("X-OMS-Internal-Key", cfg.apiKey())
                        .GET()
                        .timeout(ADMIN_TIMEOUT)
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("session GET failed " + resp.statusCode());
        }
        return resp.body().contains("\"loggedOn\":true");
    }

    private static void awaitLogon(FixInClientEmbeddedInitiator client) {
        Awaitility.await().atMost(LOGON_TIMEOUT).pollInterval(Duration.ofMillis(250)).until(client::isLoggedOn);
    }

    private static void assertReceivedErCount(String clOrdId, int expected) {
        long count = ersForClOrdId(clOrdId).size();
        if (count < expected) {
            throw new AssertionError("expected " + expected + " ERs for " + clOrdId + ", got " + count);
        }
    }

    private static void assertBmrCountAtLeast(int expected) {
        if (FixInClientCollectorApplication.BUSINESS_REJECTS.size() < expected) {
            throw new AssertionError(
                    "expected >= " + expected + " BMRs, got " + FixInClientCollectorApplication.BUSINESS_REJECTS.size());
        }
    }

    private static void assertRateLimitRejectObserved() {
        if (!FixInClientCollectorApplication.BUSINESS_REJECTS.isEmpty()
                || !FixInClientCollectorApplication.SESSION_REJECTS.isEmpty()) {
            return;
        }
        throw new AssertionError("expected BMR or session Reject for stale/rate-limited message");
    }

    private static List<FixInClientCollectorApplication.ReceivedEr> ersForClOrdId(String clOrdId) {
        return FixInClientCollectorApplication.RECEIVED.stream()
                .filter(er -> clOrdId.equals(er.clOrdId()))
                .toList();
    }

    private static ScenarioResult pass(String name, String detail) {
        log.info("PASS {} — {}", name, detail);
        return new ScenarioResult(name, true, detail);
    }

    private static ScenarioResult fail(String name, String detail) {
        log.warn("FAIL {} — {}", name, detail);
        return new ScenarioResult(name, false, detail);
    }

    private static void writeReport(Path path, List<ScenarioResult> results) throws Exception {
        Files.createDirectories(path.getParent());
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.put("finishedAt", java.time.Instant.now().toString());
        ArrayNode scenarios = root.putArray("scenarios");
        int pass = 0;
        int fail = 0;
        for (ScenarioResult r : results) {
            ObjectNode row = scenarios.addObject();
            row.put("scenario", r.name());
            row.put("status", r.passed() ? "pass" : "fail");
            row.put("detail", r.detail());
            if (r.passed()) {
                pass++;
            } else {
                fail++;
            }
        }
        root.put("passed", pass);
        root.put("failed", fail);
        mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), root);
        log.info("Conformance probe report written to {}", path.toAbsolutePath());
    }

    private record ScenarioResult(String name, boolean passed, String detail) {}

    private record ProbeConfig(
            String connectHost,
            int connectPort,
            String orderEntrySender,
            String dropCopySender,
            String target,
            String ingressUrl,
            String apiKey,
            String orderEntrySessionId,
            Path reportPath,
            Path storeBase,
            long staleMs) {

        static ProbeConfig fromEnv() {
            return new ProbeConfig(
                    env("FIX_IN_CLIENT_CONNECT_HOST", DEFAULT_HOST),
                    intEnv("FIX_IN_CLIENT_CONNECT_PORT", DEFAULT_PORT),
                    env("FIX_IN_CLIENT_SENDER", DEFAULT_ORDER_ENTRY_SENDER),
                    env("FIX_IN_DROP_COPY_SENDER", DEFAULT_DROP_COPY_SENDER),
                    env("FIX_IN_CLIENT_TARGET", DEFAULT_TARGET),
                    env("OMS_FIX_INGRESS_INTERNAL_BASE_URL", env("OMS_INTERNAL_BASE_URL", DEFAULT_INGRESS_URL)),
                    requireEnv("OMS_INTERNAL_API_KEY"),
                    env("FIX_IN_ORDER_ENTRY_SESSION_ID", DEFAULT_ORDER_ENTRY_SESSION_ID),
                    Paths.get(env("FIX_IN_CONFORMANCE_REPORT_PATH", DEFAULT_REPORT)).toAbsolutePath(),
                    Paths.get(env("FIX_IN_PROBE_FILE_STORE", "./build/fix-in-conformance-probe")).toAbsolutePath(),
                    longEnv("FIX_IN_PROBE_STALE_MS", DEFAULT_STALE_MS));
        }

        HttpClient httpClient() {
            return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }

        Path probeStorePath(String suffix) {
            return storeBase.resolve(suffix);
        }

        private static String env(String key, String def) {
            String v = System.getenv(key);
            return v == null || v.isBlank() ? def : v.trim();
        }

        private static String requireEnv(String key) {
            String v = System.getenv(key);
            if (v == null || v.isBlank()) {
                throw new IllegalStateException("missing env " + key);
            }
            return v.trim();
        }

        private static int intEnv(String key, int def) {
            String v = System.getenv(key);
            if (v == null || v.isBlank()) {
                return def;
            }
            return Integer.parseInt(v.trim());
        }

        private static long longEnv(String key, long def) {
            String v = System.getenv(key);
            if (v == null || v.isBlank()) {
                return def;
            }
            return Long.parseLong(v.trim());
        }
    }

    private FixInLoopbackConformanceProbeMain() {}
}
