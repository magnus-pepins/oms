package com.balh.oms.fix;

import org.junit.jupiter.api.Test;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.IncorrectTagValue;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.ScreenLogFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves QuickFIX/J is on the classpath and a minimal acceptor/initiator pair can log on. The
 * end-to-end FIX path (cluster → {@code oms-fix-egress} → broker → {@code FixInboundClusterSink} →
 * cluster {@code ApplyExecutionReportCommand} → projector) is covered by {@code OmsFixEgressBrokerIT}
 * + {@code OmsFixEgressInboundErRoundTripIT}; this test only proves the QuickFIX/J handshake.
 */
class FixLogonSmokeTest {

    @Test
    void acceptorAndInitiatorCompleteLogon() throws Exception {
        int port = freeTcpPort();
        Path root = Files.createTempDirectory("oms-qfj-smoke");

        SessionSettings acceptorSettings = sessionSettings(port, true, "ACCEPTOR", "INITIATOR", root.resolve("acc"));
        SessionSettings initiatorSettings = sessionSettings(port, false, "INITIATOR", "ACCEPTOR", root.resolve("ini"));

        CountDownLatch acceptorLogon = new CountDownLatch(1);
        CountDownLatch initiatorLogon = new CountDownLatch(1);

        Application acceptorApp = latchApp(acceptorLogon);
        Application initiatorApp = latchApp(initiatorLogon);

        MessageStoreFactory accStore = new FileStoreFactory(acceptorSettings);
        LogFactory accLog = new ScreenLogFactory(acceptorSettings);
        MessageStoreFactory iniStore = new FileStoreFactory(initiatorSettings);
        LogFactory iniLog = new ScreenLogFactory(initiatorSettings);
        DefaultMessageFactory msgFactory = new DefaultMessageFactory();

        SocketAcceptor acceptor = new SocketAcceptor(acceptorApp, accStore, acceptorSettings, accLog, msgFactory);
        SocketInitiator initiator = new SocketInitiator(initiatorApp, iniStore, initiatorSettings, iniLog, msgFactory);

        acceptor.start();
        try {
            initiator.start();
            assertThat(initiatorLogon.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(acceptorLogon.await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            initiator.stop();
            acceptor.stop();
        }
    }

    /**
     * Exit-gate §14.6: with {@code ResetOnLogon=Y} on both sides, a disconnect/reconnect cycle must complete a
     * second logon (sequence reset contract for day rollover / test acceptors).
     */
    @Test
    void reconnectCompletesSecondLogonWhenResetOnLogonEnabled() throws Exception {
        int port = freeTcpPort();
        Path root = Files.createTempDirectory("oms-qfj-reset-on-logon");

        SessionSettings acceptorSettings =
                sessionSettings(port, true, "ACCEPTOR", "INITIATOR", root.resolve("acc"), true);
        SessionSettings initiatorSettings =
                sessionSettings(port, false, "INITIATOR", "ACCEPTOR", root.resolve("ini"), true);

        AtomicReference<CountDownLatch> acceptorWave = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CountDownLatch> initiatorWave = new AtomicReference<>(new CountDownLatch(1));

        Application acceptorApp = waveLatchApp(acceptorWave);
        Application initiatorApp = waveLatchApp(initiatorWave);

        MessageStoreFactory accStore = new FileStoreFactory(acceptorSettings);
        LogFactory accLog = new ScreenLogFactory(acceptorSettings);
        MessageStoreFactory iniStore = new FileStoreFactory(initiatorSettings);
        LogFactory iniLog = new ScreenLogFactory(initiatorSettings);
        DefaultMessageFactory msgFactory = new DefaultMessageFactory();

        SocketAcceptor acceptor = new SocketAcceptor(acceptorApp, accStore, acceptorSettings, accLog, msgFactory);
        SocketInitiator initiator = new SocketInitiator(initiatorApp, iniStore, initiatorSettings, iniLog, msgFactory);

        acceptor.start();
        try {
            initiator.start();
            assertThat(initiatorWave.get().await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(acceptorWave.get().await(15, TimeUnit.SECONDS)).isTrue();

            initiator.stop();
            acceptorWave.set(new CountDownLatch(1));
            initiatorWave.set(new CountDownLatch(1));

            initiator.start();
            assertThat(initiatorWave.get().await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(acceptorWave.get().await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            initiator.stop();
            acceptor.stop();
        }
    }

    private static Application waveLatchApp(AtomicReference<CountDownLatch> wave) {
        return new Application() {
            @Override
            public void onCreate(SessionID sessionId) {}

            @Override
            public void onLogon(SessionID sessionId) {
                CountDownLatch latch = wave.get();
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public void onLogout(SessionID sessionId) {}

            @Override
            public void toAdmin(Message message, SessionID sessionId) {}

            @Override
            public void fromAdmin(Message message, SessionID sessionId)
                    throws FieldNotFound, IncorrectTagValue, RejectLogon {}

            @Override
            public void toApp(Message message, SessionID sessionId) {}

            @Override
            public void fromApp(Message message, SessionID sessionId)
                    throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
                new MessageCracker() {}.crack(message, sessionId);
            }
        };
    }

    private static Application latchApp(CountDownLatch logonLatch) {
        return new Application() {
            @Override
            public void onCreate(SessionID sessionId) { }

            @Override
            public void onLogon(SessionID sessionId) {
                logonLatch.countDown();
            }

            @Override
            public void onLogout(SessionID sessionId) { }

            @Override
            public void toAdmin(Message message, SessionID sessionId) { }

            @Override
            public void fromAdmin(Message message, SessionID sessionId)
                    throws FieldNotFound, IncorrectTagValue, RejectLogon { }

            @Override
            public void toApp(Message message, SessionID sessionId) { }

            @Override
            public void fromApp(Message message, SessionID sessionId)
                    throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
                new MessageCracker() { }.crack(message, sessionId);
            }
        };
    }

    private static SessionSettings sessionSettings(
            int port, boolean acceptor, String sender, String target, Path fileStorePath)
            throws ConfigError, IOException {
        return sessionSettings(port, acceptor, sender, target, fileStorePath, false);
    }

    private static SessionSettings sessionSettings(
            int port, boolean acceptor, String sender, String target, Path fileStorePath, boolean resetOnLogon)
            throws ConfigError, IOException {
        Files.createDirectories(fileStorePath);
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", acceptor ? "acceptor" : "initiator");
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "23:59:59");
        s.setString("HeartBtInt", "30");
        s.setString("ReconnectInterval", "5");
        s.setString("FileStorePath", fileStorePath.toString());
        s.setString("UseDataDictionary", "N");
        if (acceptor) {
            s.setString("SocketAcceptPort", String.valueOf(port));
        } else {
            s.setString("SocketConnectHost", "127.0.0.1");
            s.setString("SocketConnectPort", String.valueOf(port));
        }
        SessionID sid = new SessionID(
                new BeginString("FIX.4.4"),
                new SenderCompID(sender),
                new TargetCompID(target));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", sender);
        s.setString(sid, "TargetCompID", target);
        if (resetOnLogon) {
            s.setString(sid, "ResetOnLogon", "Y");
        }
        return s;
    }

    private static int freeTcpPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
