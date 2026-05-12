package com.balh.oms.fixegress;

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
 * Loopback FIX 4.4 acceptor for {@link OmsFixEgressBrokerIT}, registered as a
 * {@link SmartLifecycle} bean with phase {@link Integer#MIN_VALUE} so it binds the loopback port
 * <strong>before</strong> the OMS {@code FixInitiatorManager} starts (which connects to the
 * port). Same shape as {@code FixRoundTripEmbeddedAcceptor}, but parameterised on the
 * {@link Application} to plug in the slice-3b-2-only counting acceptor that does not reply with
 * an {@code ExecutionReport} (slice 3d adds the inbound ER path).
 */
public final class EgressBrokerEmbeddedAcceptor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EgressBrokerEmbeddedAcceptor.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int port;
    private final Path fileStorePath;
    private final Application application;
    private final String senderCompId;
    private final String targetCompId;
    private volatile SocketAcceptor acceptor;

    public EgressBrokerEmbeddedAcceptor(int port, Path fileStorePath, Application application) {
        this(port, fileStorePath, application, "ACCEPTOR", "INITIATOR");
    }

    /**
     * Slice 3d overload: round-trip IT runs in the same JVM as the slice-3b-2 IT; QuickFIX/J's
     * session registry is JVM-global and rejects two sessions with the same {@code SessionID},
     * so the round-trip IT picks distinct CompIDs ({@code ACCEPTOR_RT} / {@code INITIATOR_RT}) and
     * the acceptor's session settings must match.
     */
    public EgressBrokerEmbeddedAcceptor(
            int port, Path fileStorePath, Application application, String senderCompId, String targetCompId) {
        this.port = port;
        this.fileStorePath = fileStorePath;
        this.application = application;
        this.senderCompId = senderCompId;
        this.targetCompId = targetCompId;
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
            SessionSettings settings = acceptorSettings(port, fileStorePath, senderCompId, targetCompId);
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new ScreenLogFactory(settings);
            acceptor = new SocketAcceptor(application, storeFactory, settings, logFactory, new DefaultMessageFactory());
            acceptor.start();
            log.info("egress IT embedded acceptor listening on port {}", port);
        } catch (ConfigError e) {
            running.set(false);
            throw new IllegalStateException("egress IT acceptor start failed", e);
        } catch (IOException e) {
            running.set(false);
            throw new IllegalStateException("egress IT acceptor file store init failed", e);
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
            log.info("egress IT embedded acceptor stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private static SessionSettings acceptorSettings(
            int port, Path fileStorePath, String senderCompId, String targetCompId) throws ConfigError {
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
                new SenderCompID(senderCompId),
                new TargetCompID(targetCompId));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", senderCompId);
        s.setString(sid, "TargetCompID", targetCompId);
        return s;
    }
}
