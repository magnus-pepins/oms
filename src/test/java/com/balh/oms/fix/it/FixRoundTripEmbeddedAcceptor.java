package com.balh.oms.fix.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Binds a loopback FIX acceptor before the OMS initiator starts ({@link SmartLifecycle} phase ordering).
 */
public final class FixRoundTripEmbeddedAcceptor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FixRoundTripEmbeddedAcceptor.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int port;
    private final Path fileStorePath;
    private volatile SocketAcceptor acceptor;

    public FixRoundTripEmbeddedAcceptor(int port, Path fileStorePath) {
        this.port = port;
        this.fileStorePath = fileStorePath;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Files.createDirectories(fileStorePath);
            SessionSettings settings = acceptorSettings(port, fileStorePath);
            Application app = new FixRoundTripAcceptorApplication();
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new ScreenLogFactory(settings);
            acceptor = new SocketAcceptor(app, storeFactory, settings, logFactory, new DefaultMessageFactory());
            acceptor.start();
            log.info("FIX IT embedded acceptor listening on port {}", port);
        } catch (ConfigError e) {
            running.set(false);
            throw new IllegalStateException("FIX acceptor start failed", e);
        } catch (IOException e) {
            running.set(false);
            throw new IllegalStateException("FIX acceptor file store init failed", e);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        SocketAcceptor local = acceptor;
        acceptor = null;
        if (local != null) {
            local.stop();
            log.info("FIX IT embedded acceptor stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private static SessionSettings acceptorSettings(int port, Path fileStorePath) throws ConfigError {
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
                new BeginString("FIX.4.4"),
                new SenderCompID("ACCEPTOR"),
                new TargetCompID("INITIATOR"));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", "ACCEPTOR");
        s.setString(sid, "TargetCompID", "INITIATOR");
        return s;
    }
}
