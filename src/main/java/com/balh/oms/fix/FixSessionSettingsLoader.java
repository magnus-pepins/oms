package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
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

/**
 * Builds {@link SessionSettings} for a single FIX 4.4 initiator session from {@link OmsConfig.Fix}.
 */
final class FixSessionSettingsLoader {

    /** QuickFIX/J initiator TLS — see usage manual {@code SocketUseSSL}. */
    private static final String QFJ_SOCKET_USE_SSL = "SocketUseSSL";
    private static final String QFJ_SOCKET_KEY_STORE = "SocketKeyStore";
    private static final String QFJ_SOCKET_KEY_STORE_PASSWORD = "SocketKeyStorePassword";
    private static final String QFJ_SOCKET_TRUST_STORE = "SocketTrustStore";
    private static final String QFJ_SOCKET_TRUST_STORE_PASSWORD = "SocketTrustStorePassword";
    private static final String QFJ_ENABLED_PROTOCOLS = "EnabledProtocols";

    private FixSessionSettingsLoader() {
    }

    static SessionSettings loadInitiatorSettings(OmsConfig omsConfig, Path fileStorePath) throws ConfigError, IOException {
        Files.createDirectories(fileStorePath);
        OmsConfig.Fix fix = omsConfig.getFix();
        SessionSettings s = new SessionSettings();
        s.setString("ConnectionType", "initiator");
        s.setString("StartTime", "00:00:00");
        s.setString("EndTime", "23:59:59");
        s.setString("HeartBtInt", String.valueOf(fix.getHeartBtInt()));
        s.setString("ReconnectInterval", "5");
        s.setString("FileStorePath", fileStorePath.toString());
        // Slice 4l: per-message fsync (QuickFIX FileStoreSync=Y) is QuickFIX/J's documented default
        // and gives crash-recovery via the on-disk message stream alone. Operators may flip to N
        // on slower storage; FIX MsgSeqNum + broker resend on logon still gives a protocol-loss-free
        // recovery path. See OmsConfig.Fix.fileStoreSync javadoc.
        s.setString("FileStoreSync", fix.getFileStoreSync());
        s.setString("UseDataDictionary", fix.isUseDataDictionary() ? "Y" : "N");
        s.setString("SocketConnectHost", fix.getSocketConnectHost());
        s.setString("SocketConnectPort", String.valueOf(fix.getSocketConnectPort()));
        SessionID sid = new SessionID(
                new BeginString("FIX.4.4"),
                new SenderCompID(fix.getSenderCompId()),
                new TargetCompID(fix.getTargetCompId()));
        s.setString(sid, "BeginString", "FIX.4.4");
        s.setString(sid, "SenderCompID", fix.getSenderCompId());
        s.setString(sid, "TargetCompID", fix.getTargetCompId());
        if (fix.isJdbcSessionStore()) {
            s.setString(sid, JdbcSetting.SETTING_JDBC_STORE_SESSIONS_TABLE_NAME, FixJdbcSessionSchema.SESSIONS_TABLE);
            s.setString(sid, JdbcSetting.SETTING_JDBC_STORE_MESSAGES_TABLE_NAME, FixJdbcSessionSchema.MESSAGES_TABLE);
        }
        if (fix.isSocketUseSsl()) {
            s.setString(sid, QFJ_SOCKET_USE_SSL, "Y");
            if (nonBlank(fix.getSocketKeyStore())) {
                s.setString(sid, QFJ_SOCKET_KEY_STORE, fix.getSocketKeyStore());
            }
            if (nonBlank(fix.getSocketKeyStorePassword())) {
                s.setString(sid, QFJ_SOCKET_KEY_STORE_PASSWORD, fix.getSocketKeyStorePassword());
            }
            if (nonBlank(fix.getSocketTrustStore())) {
                s.setString(sid, QFJ_SOCKET_TRUST_STORE, fix.getSocketTrustStore());
            }
            if (nonBlank(fix.getSocketTrustStorePassword())) {
                s.setString(sid, QFJ_SOCKET_TRUST_STORE_PASSWORD, fix.getSocketTrustStorePassword());
            }
            if (nonBlank(fix.getEnabledSslProtocols())) {
                s.setString(sid, QFJ_ENABLED_PROTOCOLS, fix.getEnabledSslProtocols());
            }
        }
        return s;
    }

    private static boolean nonBlank(String v) {
        return v != null && !v.isBlank();
    }
}
