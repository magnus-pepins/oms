package com.balh.oms.cluster.retention;

import java.util.concurrent.TimeUnit;

/** Env-driven configuration for {@link ClusterRetentionRegulator}. */
public final class ClusterRetentionConfig {

    public static final String ENV_ENABLED = "AERON_RETENTION_ENABLED";
    public static final String ENV_INTERVAL_MS = "AERON_RETENTION_INTERVAL_MS";
    public static final String ENV_SNAPSHOTS_TO_RETAIN = "AERON_RETENTION_SNAPSHOTS_TO_RETAIN";
    public static final String ENV_SHIP_ROOT = "AERON_ARCHIVE_SHIP_ROOT";
    public static final String ENV_PG_URL = "AERON_RETENTION_PG_URL";
    public static final String ENV_PG_USER = "AERON_RETENTION_PG_USER";
    public static final String ENV_PG_PASSWORD = "AERON_RETENTION_PG_PASSWORD";
    public static final String ENV_PROJECTOR_ID = "AERON_RETENTION_PROJECTOR_ID";
    public static final String ENV_EVENTS_STREAM_ID = "AERON_RETENTION_EVENTS_STREAM_ID";
    public static final String ENV_SEGMENT_FILE_LENGTH_BYTES = "AERON_ARCHIVE_SEGMENT_FILE_LENGTH_BYTES";

    public static final long DEFAULT_INTERVAL_MS = TimeUnit.HOURS.toMillis(1L);
    public static final int DEFAULT_SNAPSHOTS_TO_RETAIN = 3;
    public static final long DEFAULT_SEGMENT_FILE_LENGTH_BYTES = 64L * 1024 * 1024L;

    private final boolean enabled;
    private final long intervalMs;
    private final int snapshotsToRetain;
    private final String shipRoot;
    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;
    private final String projectorId;
    private final int eventsStreamId;
    private final long segmentFileLengthBytes;

    public ClusterRetentionConfig(
            boolean enabled,
            long intervalMs,
            int snapshotsToRetain,
            String shipRoot,
            String pgUrl,
            String pgUser,
            String pgPassword,
            String projectorId,
            int eventsStreamId,
            long segmentFileLengthBytes) {
        this.enabled = enabled;
        this.intervalMs = intervalMs;
        this.snapshotsToRetain = snapshotsToRetain;
        this.shipRoot = shipRoot;
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.projectorId = projectorId;
        this.eventsStreamId = eventsStreamId;
        this.segmentFileLengthBytes = segmentFileLengthBytes;
    }

    public static ClusterRetentionConfig fromEnv() {
        boolean enabled = Boolean.parseBoolean(envOrDefault(ENV_ENABLED, "false"));
        long intervalMs = parseLongEnv(ENV_INTERVAL_MS, DEFAULT_INTERVAL_MS);
        int snapshotsToRetain = (int) parseLongEnv(ENV_SNAPSHOTS_TO_RETAIN, DEFAULT_SNAPSHOTS_TO_RETAIN);
        String shipRoot = System.getenv(ENV_SHIP_ROOT);
        String pgUrl = System.getenv(ENV_PG_URL);
        String pgUser = System.getenv(ENV_PG_USER);
        String pgPassword = System.getenv(ENV_PG_PASSWORD);
        String projectorId = envOrDefault(ENV_PROJECTOR_ID, "oms-postgres-default");
        int eventsStreamId = (int) parseLongEnv(ENV_EVENTS_STREAM_ID, -1);
        long segmentFileLength = parseLongEnv(ENV_SEGMENT_FILE_LENGTH_BYTES, DEFAULT_SEGMENT_FILE_LENGTH_BYTES);
        return new ClusterRetentionConfig(
                enabled,
                intervalMs,
                snapshotsToRetain,
                shipRoot,
                pgUrl,
                pgUser,
                pgPassword,
                projectorId,
                eventsStreamId,
                segmentFileLength);
    }

    public boolean enabled() {
        return enabled;
    }

    public long intervalMs() {
        return intervalMs;
    }

    public int snapshotsToRetain() {
        return snapshotsToRetain;
    }

    public String shipRoot() {
        return shipRoot;
    }

    public String pgUrl() {
        return pgUrl;
    }

    public String pgUser() {
        return pgUser;
    }

    public String pgPassword() {
        return pgPassword;
    }

    public String projectorId() {
        return projectorId;
    }

    public int eventsStreamId() {
        return eventsStreamId;
    }

    public long segmentFileLengthBytes() {
        return segmentFileLengthBytes;
    }

    public boolean hasShipRoot() {
        return shipRoot != null && !shipRoot.isBlank();
    }

    public boolean hasJdbcCursorProbe() {
        return pgUrl != null && !pgUrl.isBlank()
                && pgUser != null && !pgUser.isBlank()
                && pgPassword != null;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v.trim();
    }

    private static long parseLongEnv(String key, long defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(raw.trim());
    }
}
