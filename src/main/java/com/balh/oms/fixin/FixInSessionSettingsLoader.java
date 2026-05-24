package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.fix.FixJdbcSessionSchema;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import quickfix.ConfigError;
import quickfix.JdbcSetting;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Builds QuickFIX/J {@link SessionSettings} for the FIX-in {@code SocketAcceptor}. */
final class FixInSessionSettingsLoader {

    private FixInSessionSettingsLoader() {}

    static SessionSettings loadAcceptorSettings(
            OmsConfig omsConfig, Path fileStorePath, List<FixInSessionRow> sessions)
            throws ConfigError, IOException {
        Files.createDirectories(fileStorePath);
        OmsConfig.FixIn fixIn = omsConfig.getFixIn();
        SessionSettings settings = new SessionSettings();
        settings.setString("ConnectionType", "acceptor");
        settings.setString("StartTime", "00:00:00");
        settings.setString("EndTime", "23:59:59");
        settings.setString("HeartBtInt", String.valueOf(fixIn.getHeartBtInt()));
        settings.setString("FileStorePath", fileStorePath.toString());
        settings.setString("FileStoreSync", fixIn.getFileStoreSync());
        settings.setString("UseDataDictionary", fixIn.isUseDataDictionary() ? "Y" : "N");
        settings.setString("SocketAcceptHost", fixIn.getBindHost());
        settings.setString("SocketAcceptPort", String.valueOf(fixIn.getAcceptPort()));

        if (sessions.isEmpty()) {
            throw new IllegalStateException(
                    "oms-fix-ingress requires at least one enabled row in oms_fix_in_session");
        }

        for (FixInSessionRow session : sessions) {
            String clientCompId = session.senderCompId();
            String omsCompId = session.targetCompId();
            SessionID sid = new SessionID(
                    new BeginString("FIX.4.4"),
                    new SenderCompID(omsCompId),
                    new TargetCompID(clientCompId));
            settings.setString(sid, "BeginString", "FIX.4.4");
            settings.setString(sid, "SenderCompID", omsCompId);
            settings.setString(sid, "TargetCompID", clientCompId);
            settings.setString(sid, "HeartBtInt", String.valueOf(session.heartbeatSeconds()));
            if (session.sessionQualifierOrNull() != null && !session.sessionQualifierOrNull().isBlank()) {
                settings.setString(sid, "SessionQualifier", session.sessionQualifierOrNull());
            }
            if (fixIn.isJdbcSessionStore()) {
                settings.setString(
                        sid, JdbcSetting.SETTING_JDBC_STORE_SESSIONS_TABLE_NAME, FixJdbcSessionSchema.SESSIONS_TABLE);
                settings.setString(
                        sid, JdbcSetting.SETTING_JDBC_STORE_MESSAGES_TABLE_NAME, FixJdbcSessionSchema.MESSAGES_TABLE);
            }
        }
        return settings;
    }
}
