package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
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
import quickfix.SocketInitiator;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts/stops QuickFIX/J {@link SocketInitiator} for the OMS FIX session (slice 4).
 */
public class FixInitiatorManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(FixInitiatorManager.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final OmsConfig omsConfig;
    private final OmsFixApplication application;
    private final DataSource dataSource;

    private volatile SocketInitiator initiator;

    public FixInitiatorManager(OmsConfig omsConfig, OmsFixApplication application, DataSource dataSource) {
        this.omsConfig = omsConfig;
        this.application = application;
        this.dataSource = dataSource;
    }

    /**
     * Runs after embedded acceptors registered with lower phase (e.g. slice-4 round-trip IT).
     */
    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Path store = Path.of(omsConfig.getFix().getFileStorePath()).toAbsolutePath().normalize();
            SessionSettings settings = FixSessionSettingsLoader.loadInitiatorSettings(omsConfig, store);
            MessageStoreFactory storeFactory;
            if (omsConfig.getFix().isJdbcSessionStore()) {
                var jdbcFactory = new JdbcStoreFactory(settings);
                jdbcFactory.setDataSource(dataSource);
                storeFactory = jdbcFactory;
                log.info("FIX message store: JdbcStoreFactory (sessionsTable={}, messagesTable={})",
                        FixJdbcSessionSchema.SESSIONS_TABLE, FixJdbcSessionSchema.MESSAGES_TABLE);
            } else {
                storeFactory = new FileStoreFactory(settings);
                log.info("FIX message store: FileStoreFactory (storePath={})", store);
            }
            LogFactory logFactory = new ScreenLogFactory(settings);
            DefaultMessageFactory messageFactory = new DefaultMessageFactory();
            initiator = new SocketInitiator(application, storeFactory, settings, logFactory, messageFactory);
            initiator.start();
            log.info("FIX SocketInitiator started");
        } catch (ConfigError | IOException e) {
            running.set(false);
            throw new IllegalStateException("FIX initiator start failed", e);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        SocketInitiator local = initiator;
        initiator = null;
        if (local != null) {
            local.stop();
            log.info("FIX SocketInitiator stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
