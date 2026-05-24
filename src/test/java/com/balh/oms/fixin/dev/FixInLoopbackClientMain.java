package com.balh.oms.fixin.dev;

import com.balh.oms.fixin.it.FixInClientCollectorApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FileStoreFactory;
import quickfix.Initiator;
import quickfix.LogFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone FIX 4.4 <strong>initiator</strong> for bench/UAT: persistent logon to
 * {@code oms-fix-ingress} as the seeded loopback counterparty ({@code LOOPBACK_CLIENT} →
 * {@code BALH_OMS}). Used on pop for mutating soak reconnect probes and Ops Console session
 * visibility.
 *
 * <p>Run:
 *
 * <pre>
 *   ./gradlew fixInLoopbackClient
 * </pre>
 *
 * <ul>
 *   <li>{@code FIX_IN_CLIENT_CONNECT_HOST} — default {@code 127.0.0.1}
 *   <li>{@code FIX_IN_CLIENT_CONNECT_PORT} — default {@code 9877}
 *   <li>{@code FIX_IN_CLIENT_SENDER} — client {@code SenderCompID} (default {@code LOOPBACK_CLIENT})
 *   <li>{@code FIX_IN_CLIENT_TARGET} — OMS {@code TargetCompID} (default {@code BALH_OMS})
 *   <li>{@code FIX_IN_CLIENT_FILE_STORE} — QuickFIX file store (default {@code ./build/fix-in-loopback-client})
 *   <li>{@code FIX_IN_LOOPBACK_HEARTBEAT_LOG_SECS} — periodic logged-on heartbeat log (default {@code 60}, 0=off)
 * </ul>
 */
public final class FixInLoopbackClientMain {

    private static final Logger log = LoggerFactory.getLogger(FixInLoopbackClientMain.class);

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9877;
    private static final String DEFAULT_SENDER = "LOOPBACK_CLIENT";
    private static final String DEFAULT_TARGET = "BALH_OMS";
    private static final String DEFAULT_FILE_STORE = "./build/fix-in-loopback-client";
    private static final int DEFAULT_HEARTBEAT_LOG_SECS = 60;

    public static void main(String[] args) throws Exception {
        String host = envOrDefault("FIX_IN_CLIENT_CONNECT_HOST", DEFAULT_HOST);
        int port = parseIntEnv("FIX_IN_CLIENT_CONNECT_PORT", DEFAULT_PORT);
        String sender = envOrDefault("FIX_IN_CLIENT_SENDER", DEFAULT_SENDER);
        String target = envOrDefault("FIX_IN_CLIENT_TARGET", DEFAULT_TARGET);
        Path fileStore = Paths.get(envOrDefault("FIX_IN_CLIENT_FILE_STORE", DEFAULT_FILE_STORE)).toAbsolutePath();
        int heartbeatLogSecs = parseIntEnv("FIX_IN_LOOPBACK_HEARTBEAT_LOG_SECS", DEFAULT_HEARTBEAT_LOG_SECS);

        Files.createDirectories(fileStore);
        SessionSettings settings = initiatorSettings(host, port, fileStore, sender, target);
        Application app = new FixInClientCollectorApplication();
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(settings);
        Initiator initiator = new SocketInitiator(app, storeFactory, settings, logFactory, new DefaultMessageFactory());

        SessionID sessionId =
                new SessionID(new BeginString("FIX.4.4"), new SenderCompID(sender), new TargetCompID(target));

        CountDownLatch stopped = new CountDownLatch(1);
        ScheduledExecutorService heartbeat = null;
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    log.info("shutdown hook: stopping FIX-in loopback client");
                    try {
                        initiator.stop();
                    } finally {
                        stopped.countDown();
                    }
                }));

        initiator.start();
        log.info(
                "FIX-in loopback client started host={}:{} SenderCompID={} TargetCompID={} fileStore={}",
                host,
                port,
                sender,
                target,
                fileStore);

        if (heartbeatLogSecs > 0) {
            heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fix-in-loopback-heartbeat");
                t.setDaemon(true);
                return t;
            });
            heartbeat.scheduleAtFixedRate(
                    () -> {
                        Session session = Session.lookupSession(sessionId);
                        boolean loggedOn = session != null && session.isLoggedOn();
                        log.info("FIX-in loopback client heartbeat loggedOn={} erCount={}", loggedOn, FixInClientCollectorApplication.RECEIVED.size());
                    },
                    heartbeatLogSecs,
                    heartbeatLogSecs,
                    TimeUnit.SECONDS);
        }

        stopped.await();
        if (heartbeat != null) {
            heartbeat.shutdownNow();
        }
    }

    private static SessionSettings initiatorSettings(
            String host, int port, Path fileStorePath, String sender, String target) throws ConfigError {
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "initiator");
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "23:59:59");
        s.setString("HeartBtInt", "30");
        s.setString("ReconnectInterval", "5");
        s.setString("FileStorePath", fileStorePath.toString());
        s.setString("UseDataDictionary", "N");
        s.setString("SocketConnectHost", host);
        s.setString("SocketConnectPort", String.valueOf(port));
        SessionID sid = new SessionID(new BeginString("FIX.4.4"), new SenderCompID(sender), new TargetCompID(target));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", sender);
        s.setString(sid, "TargetCompID", target);
        s.setString(sid, "ResetOnLogon", "Y");
        return s;
    }

    private static String envOrDefault(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    private static int parseIntEnv(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        return Integer.parseInt(v.trim());
    }

    private FixInLoopbackClientMain() {}
}
