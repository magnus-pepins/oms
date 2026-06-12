package com.balh.oms.cluster.retention;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader-side daemon: ship detached segments off-host, then {@link AeronArchive#purgeSegments}
 * on the cluster log (and optional events recording) while the cluster is live.
 */
public final class ClusterRetentionRegulator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterRetentionRegulator.class);

    public static final String METRIC_ARCHIVE_BYTES = "oms_cluster_archive_bytes_on_disk";
    public static final String METRIC_UNSHIPPED_LAG = "oms_cluster_archive_unshipped_lag_bytes";
    public static final String METRIC_PURGE_RUNS = "oms_cluster_retention_purge_runs_total";
    public static final String METRIC_SEGMENTS_RECLAIMED = "oms_cluster_retention_segments_reclaimed_total";

    private final ClusterRetentionConfig config;
    private final File clusterDir;
    private final File archiveDir;
    private final String aeronDirectory;
    private final String clusterLabel;
    private final Thread regulatorThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ArchiveShipStateStore shipState;
    private final FileArchiveShipper shipper;
    private final AtomicLong archiveBytesGauge = new AtomicLong(0L);
    private final AtomicLong unshippedLagGauge = new AtomicLong(0L);
    private final Counter purgeRunsCounter;
    private final Counter segmentsReclaimedCounter;

    public ClusterRetentionRegulator(
            ClusterRetentionConfig config,
            File clusterDir,
            File archiveDir,
            String aeronDirectory,
            String clusterLabel,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.clusterDir = clusterDir;
        this.archiveDir = archiveDir;
        this.aeronDirectory = aeronDirectory;
        this.clusterLabel = clusterLabel;
        this.shipState = new ArchiveShipStateStore(archiveDir);
        this.shipper =
                config.hasShipRoot() ? new FileArchiveShipper(new File(config.shipRoot()), clusterLabel) : null;
        this.regulatorThread = new Thread(this::loop, "ledger-cluster-retention-regulator");
        this.regulatorThread.setDaemon(true);
        if (meterRegistry != null) {
            Gauge.builder(METRIC_ARCHIVE_BYTES, archiveBytesGauge, AtomicLong::get)
                    .description("Bytes on disk under the ledger cluster archive directory.")
                    .register(meterRegistry);
            Gauge.builder(METRIC_UNSHIPPED_LAG, unshippedLagGauge, AtomicLong::get)
                    .description("Bytes not yet confirmed shipped off-host for the active log recording.")
                    .register(meterRegistry);
            this.purgeRunsCounter =
                    Counter.builder(METRIC_PURGE_RUNS).register(meterRegistry);
            this.segmentsReclaimedCounter =
                    Counter.builder(METRIC_SEGMENTS_RECLAIMED).register(meterRegistry);
        } else {
            this.purgeRunsCounter = null;
            this.segmentsReclaimedCounter = null;
        }
    }

    public void start() {
        if (!config.enabled()) {
            log.info("cluster retention regulator disabled");
            return;
        }
        if (!config.hasShipRoot()) {
            log.warn("cluster retention enabled but {} is unset — purge will refuse until ship root exists",
                    ClusterRetentionConfig.ENV_SHIP_ROOT);
        }
        if (running.compareAndSet(false, true)) {
            log.info(
                    "starting cluster retention regulator: clusterDir='{}' intervalMs={} snapshotsToRetain={}",
                    clusterDir.getAbsolutePath(),
                    config.intervalMs(),
                    config.snapshotsToRetain());
            regulatorThread.start();
        }
    }

    public boolean runOnceForTest() {
        return runRetentionPass();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            regulatorThread.interrupt();
            try {
                regulatorThread.join(TimeUnit.SECONDS.toMillis(5L));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    runRetentionPass();
                } catch (RuntimeException ex) {
                    log.warn("retention pass failed", ex);
                }
                Thread.sleep(config.intervalMs());
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean runRetentionPass() {
        updateArchiveBytesGauge();
        OptionalLong snapshotFloor =
                ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(
                        clusterDir, config.snapshotsToRetain());
        if (snapshotFloor.isEmpty()) {
            log.debug("retention pass skipped: no retained snapshot floor");
            return false;
        }
        if (!config.hasShipRoot() || shipper == null) {
            log.debug("retention pass skipped: ship root not configured");
            return false;
        }

        long consumerFloor = Long.MAX_VALUE;
        if (config.hasJdbcCursorProbe()) {
            try (JdbcConsumerCursorProbe probe =
                    new JdbcConsumerCursorProbe(
                            config.pgUrl(), config.pgUser(), config.pgPassword(), config.projectorId())) {
                OptionalLong cursor = probe.minConsumerPosition();
                if (cursor.isEmpty()) {
                    log.debug("retention pass skipped: consumer cursor unknown");
                    return false;
                }
                consumerFloor = cursor.getAsLong();
            } catch (Exception ex) {
                log.warn("retention pass skipped: JDBC cursor probe failed", ex);
                return false;
            }
        }

        Aeron aeron = null;
        AeronArchive archive = null;
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel("aeron:ipc?term-length=64k")
                    .controlResponseChannel("aeron:ipc?term-length=64k"));

            long logRecordingId = findActiveLogRecordingId();
            if (logRecordingId < 0L) {
                log.debug("retention pass skipped: no log recording id");
                return false;
            }

            long startPosition = archive.getStartPosition(logRecordingId);
            long recordingPosition = archive.getRecordingPosition(logRecordingId);
            long shippedFloor = shipState.shippedHighWaterFor(logRecordingId);

            long purgeFloor =
                    ClusterPurgeFloorCalculator.computePurgeFloor(
                            snapshotFloor.getAsLong(),
                            consumerFloor,
                            shippedFloor,
                            config.segmentFileLengthBytes());

            if (purgeFloor <= startPosition) {
                unshippedLagGauge.set(Math.max(0L, recordingPosition - shippedFloor));
                return false;
            }

            long shipTarget = purgeFloor;
            long shipped = shipper.shipSegments(archiveDir, logRecordingId, shipTarget, config.segmentFileLengthBytes());
            if (shipped <= shippedFloor) {
                log.debug(
                        "retention pass skipped: ship did not advance (shipped={} prior={})",
                        shipped,
                        shippedFloor);
                unshippedLagGauge.set(Math.max(0L, recordingPosition - shippedFloor));
                return false;
            }
            shipState.markShipped(logRecordingId, shipped);

            long newShippedFloor = shipState.shippedHighWaterFor(logRecordingId);
            purgeFloor =
                    ClusterPurgeFloorCalculator.computePurgeFloor(
                            snapshotFloor.getAsLong(),
                            consumerFloor,
                            newShippedFloor,
                            config.segmentFileLengthBytes());

            if (purgeFloor <= startPosition) {
                return false;
            }

            long reclaimedBytes = purgeFloor - startPosition;
            archive.purgeSegments(logRecordingId, purgeFloor);
            purgeOldSnapshotRecordings(archive);
            if (purgeRunsCounter != null) {
                purgeRunsCounter.increment();
            }
            if (segmentsReclaimedCounter != null) {
                segmentsReclaimedCounter.increment(reclaimedBytes);
            }
            log.info(
                    "retention purge applied: recordingId={} purgeFloor={} reclaimedBytes~={}",
                    logRecordingId,
                    purgeFloor,
                    reclaimedBytes);
            unshippedLagGauge.set(Math.max(0L, recordingPosition - newShippedFloor));
            updateArchiveBytesGauge();
            return true;
        } finally {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    private long findActiveLogRecordingId() {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            RecordingLog.Entry latest = recordingLog.findLastTerm();
            if (latest == null) {
                return -1L;
            }
            return latest.recordingId;
        }
    }

    private void purgeOldSnapshotRecordings(AeronArchive archive) {
        List<Long> snapshotRecordingIds = new ArrayList<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    snapshotRecordingIds.add(entry.recordingId);
                }
            }
        }
        if (snapshotRecordingIds.size() <= config.snapshotsToRetain()) {
            return;
        }
        snapshotRecordingIds.sort(Long::compareTo);
        int toPurge = snapshotRecordingIds.size() - config.snapshotsToRetain();
        for (int i = 0; i < toPurge; i++) {
            long recordingId = snapshotRecordingIds.get(i);
            try {
                archive.purgeRecording(recordingId);
                log.info("purged old snapshot recordingId={}", recordingId);
            } catch (Exception ex) {
                log.warn("failed to purge old snapshot recordingId={}", recordingId, ex);
            }
        }
    }

    private void updateArchiveBytesGauge() {
        archiveBytesGauge.set(dirSizeBytes(archiveDir));
    }

    private static long dirSizeBytes(File dir) {
        if (dir == null || !dir.exists()) {
            return 0L;
        }
        long total = 0L;
        File[] children = dir.listFiles();
        if (children == null) {
            return 0L;
        }
        for (File child : children) {
            if (child.isFile()) {
                total += child.length();
            } else if (child.isDirectory()) {
                total += dirSizeBytes(child);
            }
        }
        return total;
    }
}
