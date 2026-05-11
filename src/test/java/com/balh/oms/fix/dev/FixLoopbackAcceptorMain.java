package com.balh.oms.fix.dev;

import com.balh.oms.fix.it.FixRoundTripAcceptorApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FileStoreFactory;
import quickfix.LogFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

/**
 * Standalone minimal FIX 4.4 <strong>acceptor</strong> for local dev: logs on, receives
 * {@code NewOrderSingle}, replies with the same synthetic full-fill {@code ExecutionReport} as the
 * slice-4 round-trip IT ({@link FixRoundTripAcceptorApplication}).
 *
 * <p>Run (from repo root):
 *
 * <pre>
 *   ./gradlew fixLoopbackAcceptor
 * </pre>
 *
 * <p>Defaults match OMS {@code application.yaml} initiator comp ids ({@code OMS_INIT} →
 * {@code BROKER_ACCEPT}) and port {@code 9876}. Override with env vars below so they match your
 * running OMS {@code oms.fix.*} settings exactly.
 *
 * <ul>
 *   <li>{@code FIX_ACCEPTOR_PORT} — listen port (default {@code 9876})
 *   <li>{@code FIX_ACCEPTOR_FILE_STORE} — QuickFIX file store dir (default {@code ./build/fix-loopback-acceptor})
 *   <li>{@code FIX_ACCEPTOR_SESSION_SENDER} — acceptor {@code SenderCompID} (default {@code BROKER_ACCEPT})
 *   <li>{@code FIX_ACCEPTOR_SESSION_TARGET} — acceptor {@code TargetCompID} (default {@code OMS_INIT})
 * </ul>
 */
public final class FixLoopbackAcceptorMain {

    private static final Logger log = LoggerFactory.getLogger(FixLoopbackAcceptorMain.class);

    private static final int DEFAULT_PORT = 9876;
    private static final String DEFAULT_FILE_STORE = "./build/fix-loopback-acceptor";
    /** Must match OMS {@code oms.fix.target-comp-id} (broker side on the wire). */
    private static final String DEFAULT_SESSION_SENDER = "BROKER_ACCEPT";
    /** Must match OMS {@code oms.fix.sender-comp-id} (OMS side on the wire). */
    private static final String DEFAULT_SESSION_TARGET = "OMS_INIT";

    public static void main(String[] args) throws Exception {
        int port = parseIntEnv("FIX_ACCEPTOR_PORT", DEFAULT_PORT);
        Path fileStore = Paths.get(envOrDefault("FIX_ACCEPTOR_FILE_STORE", DEFAULT_FILE_STORE)).toAbsolutePath();
        String sessionSender = envOrDefault("FIX_ACCEPTOR_SESSION_SENDER", DEFAULT_SESSION_SENDER);
        String sessionTarget = envOrDefault("FIX_ACCEPTOR_SESSION_TARGET", DEFAULT_SESSION_TARGET);

        Files.createDirectories(fileStore);
        SessionSettings settings = acceptorSettings(port, fileStore, sessionSender, sessionTarget);
        Application app = new FixRoundTripAcceptorApplication();
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(settings);
        SocketAcceptor acceptor = new SocketAcceptor(app, storeFactory, settings, logFactory, new DefaultMessageFactory());

        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    log.info("shutdown hook: stopping FIX acceptor");
                    try {
                        acceptor.stop();
                    } finally {
                        stopped.countDown();
                    }
                }));

        acceptor.start();
        log.info(
                "FIX loopback acceptor listening on port={} session SenderCompID={} TargetCompID={} fileStore={}",
                port,
                sessionSender,
                sessionTarget,
                fileStore);

        stopped.await();
    }

    private static SessionSettings acceptorSettings(int port, Path fileStorePath, String sender, String target)
            throws ConfigError {
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "acceptor");
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "23:59:59");
        s.setString("HeartBtInt", "30");
        s.setString("ReconnectInterval", "5");
        s.setString("FileStorePath", fileStorePath.toString());
        s.setString("UseDataDictionary", "N");
        s.setString("SocketAcceptPort", String.valueOf(port));
        SessionID sid = new SessionID(
                new BeginString("FIX.4.4"), new SenderCompID(sender), new TargetCompID(target));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", sender);
        s.setString(sid, "TargetCompID", target);
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

    private FixLoopbackAcceptorMain() {}
}
