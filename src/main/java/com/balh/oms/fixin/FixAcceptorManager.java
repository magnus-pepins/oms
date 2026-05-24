package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.fix.FixJdbcSessionSchema;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FileStoreFactory;
import quickfix.JdbcStoreFactory;
import quickfix.LogFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/** Starts/stops QuickFIX/J {@link SocketAcceptor} for FIX-in clients. */
public class FixAcceptorManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FixAcceptorManager.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final OmsConfig omsConfig;
    private final FixInApplication application;
    private final FixInSessionRepository sessionRepository;
    private final DataSource dataSource;

    private volatile SocketAcceptor acceptor;

    public FixAcceptorManager(
            OmsConfig omsConfig,
            FixInApplication application,
            FixInSessionRepository sessionRepository,
            DataSource dataSource) {
        this.omsConfig = omsConfig;
        this.application = application;
        this.sessionRepository = sessionRepository;
        this.dataSource = dataSource;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void start() {
        if (!omsConfig.getFixIn().isEnabled()) {
            log.info("FIX-in acceptor disabled (oms.fix-in.enabled=false)");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Path store = Path.of(omsConfig.getFixIn().getFileStorePath()).toAbsolutePath().normalize();
            SessionSettings settings =
                    FixInSessionSettingsLoader.loadAcceptorSettings(
                            omsConfig, store, sessionRepository.findEnabled());
            MessageStoreFactory storeFactory;
            if (omsConfig.getFixIn().isJdbcSessionStore()) {
                var jdbcFactory = new JdbcStoreFactory(settings);
                jdbcFactory.setDataSource(dataSource);
                storeFactory = jdbcFactory;
                log.info(
                        "FIX-in message store: JdbcStoreFactory (sessionsTable={}, messagesTable={})",
                        FixJdbcSessionSchema.SESSIONS_TABLE,
                        FixJdbcSessionSchema.MESSAGES_TABLE);
            } else {
                storeFactory = new FileStoreFactory(settings);
                log.info("FIX-in message store: FileStoreFactory (storePath={})", store);
            }
            LogFactory logFactory = new ScreenLogFactory(settings);
            acceptor = new SocketAcceptor(
                    application, storeFactory, settings, logFactory, new DefaultMessageFactory());
            acceptor.start();
            log.info(
                    "FIX-in SocketAcceptor started on {}:{}",
                    omsConfig.getFixIn().getBindHost(),
                    omsConfig.getFixIn().getAcceptPort());
        } catch (ConfigError | IOException e) {
            running.set(false);
            throw new IllegalStateException("FIX-in acceptor start failed", e);
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
            log.info("FIX-in SocketAcceptor stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
