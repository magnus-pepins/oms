package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import quickfix.ConfigError;
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
        return s;
    }
}
