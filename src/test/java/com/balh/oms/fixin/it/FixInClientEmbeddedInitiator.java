package com.balh.oms.fixin.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FileStoreFactory;
import quickfix.Initiator;
import quickfix.LogFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.BeginString;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.NewOrderSingle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicBoolean;

/** Loopback FIX-in client initiator for integration tests (connects to {@code oms-fix-ingress} acceptor). */
public final class FixInClientEmbeddedInitiator {

    private static final Logger log = LoggerFactory.getLogger(FixInClientEmbeddedInitiator.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int connectPort;
    private final Path fileStorePath;
    private final String clientCompId;
    private final String omsCompId;
    private volatile Initiator initiator;
    private volatile SessionID sessionId;

    public FixInClientEmbeddedInitiator(int connectPort, Path fileStorePath, String clientCompId, String omsCompId) {
        this.connectPort = connectPort;
        this.fileStorePath = fileStorePath;
        this.clientCompId = clientCompId;
        this.omsCompId = omsCompId;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Files.createDirectories(fileStorePath);
            SessionSettings settings = initiatorSettings();
            FixInClientCollectorApplication app = new FixInClientCollectorApplication();
            MessageStoreFactory storeFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new ScreenLogFactory(settings);
            initiator = new SocketInitiator(
                    app, storeFactory, settings, logFactory, new DefaultMessageFactory());
            initiator.start();
            sessionId = new SessionID(new BeginString("FIX.4.4"), new SenderCompID(clientCompId), new TargetCompID(omsCompId));
            log.info("FIX-in IT client initiator started → {}:{}", "127.0.0.1", connectPort);
        } catch (ConfigError | IOException e) {
            running.set(false);
            throw new IllegalStateException("FIX-in IT client initiator start failed", e);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Initiator local = initiator;
        initiator = null;
        if (local != null) {
            local.stop();
            log.info("FIX-in IT client initiator stopped");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isLoggedOn() {
        SessionID sid = sessionId;
        if (sid == null) {
            return false;
        }
        Session session = Session.lookupSession(sid);
        return session != null && session.isLoggedOn();
    }

    public void sendNewOrderSingle(String clOrdId, String symbol, double qty, double price) {
        SessionID sid = sessionId;
        if (sid == null) {
            throw new IllegalStateException("session_not_initialized");
        }
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(clOrdId));
        nos.set(new Symbol(symbol));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.LIMIT));
        nos.set(new OrderQty(qty));
        nos.set(new Price(price));
        nos.set(new TimeInForce(TimeInForce.DAY));
        nos.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        try {
            Session.sendToTarget(nos, sid);
        } catch (SessionNotFound e) {
            throw new IllegalStateException("fix_in_client_session_not_found", e);
        }
    }

    private SessionSettings initiatorSettings() throws ConfigError {
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "initiator");
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "23:59:59");
        s.setString("HeartBtInt", "30");
        s.setString("ReconnectInterval", "5");
        s.setString("FileStorePath", fileStorePath.toString());
        s.setString("UseDataDictionary", "N");
        s.setString("SocketConnectHost", "127.0.0.1");
        s.setString("SocketConnectPort", String.valueOf(connectPort));
        SessionID sid = new SessionID(
                new BeginString("FIX.4.4"),
                new SenderCompID(clientCompId),
                new TargetCompID(omsCompId));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", clientCompId);
        s.setString(sid, "TargetCompID", omsCompId);
        return s;
    }
}
