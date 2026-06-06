package com.balh.oms.cluster.disk;

import java.nio.file.Path;

/** Threshold policy for Aeron archive data directory disk pressure. */
public final class AeronDiskPressurePolicy {

    public static final String ENV_ENABLED = "AERON_DISK_PRESSURE_ENABLED";
    public static final String ENV_DATA_DIR = "AERON_DISK_DATA_DIR";
    public static final String ENV_WARN_BYTES_FREE = "AERON_DISK_WARN_BYTES_FREE";
    public static final String ENV_REJECT_BYTES_FREE = "AERON_DISK_REJECT_BYTES_FREE";
    public static final String ENV_CRITICAL_BYTES_FREE = "AERON_DISK_CRITICAL_BYTES_FREE";
    public static final String ENV_WARN_FREE_RATIO = "AERON_DISK_WARN_FREE_RATIO";
    public static final String ENV_REJECT_FREE_RATIO = "AERON_DISK_REJECT_FREE_RATIO";
    public static final String ENV_CRITICAL_FREE_RATIO = "AERON_DISK_CRITICAL_FREE_RATIO";
    public static final String ENV_POLL_INTERVAL_MS = "AERON_DISK_POLL_INTERVAL_MS";

    private static final long BYTES_PER_KIB = 1024L;
    private static final long BYTES_PER_MIB = BYTES_PER_KIB * 1024L;
    private static final long BYTES_PER_GIB = BYTES_PER_MIB * 1024L;

    private static final long DEFAULT_WARN_BYTES_FREE = 10L * BYTES_PER_GIB;
    private static final long DEFAULT_REJECT_BYTES_FREE = 5L * BYTES_PER_GIB;
    private static final long DEFAULT_CRITICAL_BYTES_FREE = 2L * BYTES_PER_GIB;
    private static final double DEFAULT_WARN_FREE_RATIO = 0.15d;
    private static final double DEFAULT_REJECT_FREE_RATIO = 0.10d;
    private static final double DEFAULT_CRITICAL_FREE_RATIO = 0.05d;
    private static final long DEFAULT_POLL_INTERVAL_MS = 5_000L;

    private final boolean enabled;
    private final Path dataDir;
    private final long warnBytesFree;
    private final long rejectBytesFree;
    private final long criticalBytesFree;
    private final double warnFreeRatio;
    private final double rejectFreeRatio;
    private final double criticalFreeRatio;
    private final long pollIntervalMs;

    public AeronDiskPressurePolicy(
            boolean enabled,
            Path dataDir,
            long warnBytesFree,
            long rejectBytesFree,
            long criticalBytesFree,
            double warnFreeRatio,
            double rejectFreeRatio,
            double criticalFreeRatio,
            long pollIntervalMs) {
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir must be set");
        }
        if (warnBytesFree < rejectBytesFree || rejectBytesFree < criticalBytesFree) {
            throw new IllegalArgumentException("threshold bytes must be warn > reject > critical");
        }
        if (warnFreeRatio < rejectFreeRatio || rejectFreeRatio < criticalFreeRatio) {
            throw new IllegalArgumentException("threshold ratios must be warn > reject > critical");
        }
        if (pollIntervalMs <= 0L) {
            throw new IllegalArgumentException("pollIntervalMs must be > 0");
        }
        this.enabled = enabled;
        this.dataDir = dataDir;
        this.warnBytesFree = warnBytesFree;
        this.rejectBytesFree = rejectBytesFree;
        this.criticalBytesFree = criticalBytesFree;
        this.warnFreeRatio = warnFreeRatio;
        this.rejectFreeRatio = rejectFreeRatio;
        this.criticalFreeRatio = criticalFreeRatio;
        this.pollIntervalMs = pollIntervalMs;
    }

    public static AeronDiskPressurePolicy fromEnv(String defaultDataDir) {
        boolean enabled = !"false".equalsIgnoreCase(System.getenv(ENV_ENABLED));
        String dataDirRaw = System.getenv(ENV_DATA_DIR);
        Path dataDir = Path.of(
                dataDirRaw != null && !dataDirRaw.isBlank() ? dataDirRaw.trim() : defaultDataDir);
        return new AeronDiskPressurePolicy(
                enabled,
                dataDir,
                parseLongEnv(ENV_WARN_BYTES_FREE, DEFAULT_WARN_BYTES_FREE),
                parseLongEnv(ENV_REJECT_BYTES_FREE, DEFAULT_REJECT_BYTES_FREE),
                parseLongEnv(ENV_CRITICAL_BYTES_FREE, DEFAULT_CRITICAL_BYTES_FREE),
                parseDoubleEnv(ENV_WARN_FREE_RATIO, DEFAULT_WARN_FREE_RATIO),
                parseDoubleEnv(ENV_REJECT_FREE_RATIO, DEFAULT_REJECT_FREE_RATIO),
                parseDoubleEnv(ENV_CRITICAL_FREE_RATIO, DEFAULT_CRITICAL_FREE_RATIO),
                parseLongEnv(ENV_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS));
    }

    public boolean enabled() {
        return enabled;
    }

    public Path dataDir() {
        return dataDir;
    }

    public long pollIntervalMs() {
        return pollIntervalMs;
    }

    public AeronDiskPressureLevel evaluate(AeronDiskSpaceSample sample) {
        AeronDiskPressureLevel byBytes = evaluateByBytes(sample.usableBytes());
        AeronDiskPressureLevel byRatio = evaluateByRatio(sample.freeRatio());
        return moreSevere(byBytes, byRatio);
    }

    AeronDiskPressureLevel evaluateByBytes(long usableBytes) {
        if (usableBytes <= criticalBytesFree) {
            return AeronDiskPressureLevel.CRITICAL;
        }
        if (usableBytes <= rejectBytesFree) {
            return AeronDiskPressureLevel.REJECT;
        }
        if (usableBytes <= warnBytesFree) {
            return AeronDiskPressureLevel.WARN;
        }
        return AeronDiskPressureLevel.OK;
    }

    AeronDiskPressureLevel evaluateByRatio(double freeRatio) {
        if (freeRatio <= criticalFreeRatio) {
            return AeronDiskPressureLevel.CRITICAL;
        }
        if (freeRatio <= rejectFreeRatio) {
            return AeronDiskPressureLevel.REJECT;
        }
        if (freeRatio <= warnFreeRatio) {
            return AeronDiskPressureLevel.WARN;
        }
        return AeronDiskPressureLevel.OK;
    }

    private static AeronDiskPressureLevel moreSevere(
            AeronDiskPressureLevel left, AeronDiskPressureLevel right) {
        return left.ordinal() >= right.ordinal() ? left : right;
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(raw.trim());
    }

    private static double parseDoubleEnv(String name, double defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Double.parseDouble(raw.trim());
    }
}
