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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-node retention daemon: ships sealed archive segments off-host, then
 * {@link AeronArchive#purgeSegments} behind the shipped-and-safe floor while the cluster is live.
 * Every node regulates its own local archive — there is no leader gate because archives are
 * per-node state.
 *
 * <p>Floors per recording:
 * <ul>
 *   <li><b>cluster log recording</b> — earliest retained snapshot log position (replay on restart
 *       starts at the latest snapshot; older snapshots provide rollback margin);</li>
 *   <li><b>events recordings</b> — minimum projector cursor from Postgres, but only when the
 *       cursor is recording-id qualified: events recordings older than the cursor's recording are
 *       fully consumed and purged whole, the cursor's own recording is purged up to the cursor
 *       position. A cursor without a recording id is ambiguous across cluster restarts and never
 *       drives an events purge;</li>
 *   <li><b>both</b> — nothing is purged that has not been confirmed shipped to the off-host ship
 *       root first.</li>
 * </ul>
 *
 * <p>Snapshot recordings beyond {@code snapshotsToRetain} (counted as whole snapshots — distinct
 * log positions — not per-service {@link RecordingLog} entries) are purged. Their RecordingLog
 * entries stay valid because the public Aeron API offers no entry invalidation; recovery only uses
 * the latest snapshot, but {@code ClusterTool describe} may reference purged recordings.
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
        this.regulatorThread = new Thread(this::loop, "oms-cluster-retention-regulator");
        this.regulatorThread.setDaemon(true);
        if (meterRegistry != null) {
            Gauge.builder(METRIC_ARCHIVE_BYTES, archiveBytesGauge, AtomicLong::get)
                    .description("Bytes on disk under the oms cluster archive directory.")
                    .register(meterRegistry);
            Gauge.builder(METRIC_UNSHIPPED_LAG, unshippedLagGauge, AtomicLong::get)
                    .description("Bytes not yet confirmed shipped off-host across regulated recordings.")
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

    /** One ship-then-purge pass. Returns true when any segment or recording was reclaimed. */
    private boolean runRetentionPass() {
        updateArchiveBytesGauge();
        if (!config.hasShipRoot() || shipper == null) {
            log.debug("retention pass skipped: ship root not configured");
            return false;
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

            boolean progressed = false;
            long unshippedLag = 0L;

            // 1) Cluster log recording — floor is the earliest retained snapshot log position.
            OptionalLong snapshotFloor =
                    ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(
                            clusterDir, config.snapshotsToRetain());
            if (snapshotFloor.isPresent()) {
                long logRecordingId = findActiveLogRecordingId();
                RecordingExtent logExtent =
                        logRecordingId < 0L ? null : describeRecording(archive, logRecordingId);
                if (logExtent != null) {
                    long candidateFloor = AeronArchive.segmentFileBasePosition(
                            logExtent.startPosition(),
                            snapshotFloor.getAsLong(),
                            logExtent.termBufferLength(),
                            logExtent.segmentFileLength());
                    progressed |= shipAndPurgeSegments(archive, logExtent, candidateFloor);
                    unshippedLag += unshippedLagFor(archive, logExtent);
                }
            } else {
                log.debug("log purge skipped: no retained snapshot floor");
            }

            // 2) Events recordings — floor is the projector cursor (recording-id qualified).
            JdbcConsumerCursorProbe.ConsumerCursor cursor = probeConsumerCursor();
            if (cursor != null && config.eventsStreamId() >= 0) {
                if (cursor.recordingId() == null) {
                    log.debug(
                            "events purge skipped: projector cursor schema has no recording id"
                                    + " (position alone is ambiguous across cluster restarts)");
                } else {
                    for (RecordingExtent extent : listRecordingsForStream(archive, config.eventsStreamId())) {
                        if (extent.recordingId() < cursor.recordingId()) {
                            progressed |= shipAndPurgeWholeRecording(archive, extent);
                        } else if (extent.recordingId() == cursor.recordingId()) {
                            long candidateFloor = AeronArchive.segmentFileBasePosition(
                                    extent.startPosition(),
                                    cursor.position(),
                                    extent.termBufferLength(),
                                    extent.segmentFileLength());
                            progressed |= shipAndPurgeSegments(archive, extent, candidateFloor);
                            unshippedLag += unshippedLagFor(archive, extent);
                        }
                        // recordings newer than the cursor's are untouched
                    }
                }
            }

            // 3) Snapshot recordings beyond the retained set.
            progressed |= purgeOldSnapshotRecordings(archive);

            unshippedLagGauge.set(unshippedLag);
            if (progressed) {
                if (purgeRunsCounter != null) {
                    purgeRunsCounter.increment();
                }
                updateArchiveBytesGauge();
            }
            return progressed;
        } finally {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    /**
     * Ships segments of {@code extent} below {@code candidateFloor}, then purges up to the part of
     * the floor confirmed shipped. Returns true when segments were purged.
     */
    private boolean shipAndPurgeSegments(AeronArchive archive, RecordingExtent extent, long candidateFloor) {
        long recordingId = extent.recordingId();
        long startPosition = archive.getStartPosition(recordingId);
        if (candidateFloor <= startPosition) {
            return false;
        }
        long priorShipped = Math.max(shipState.shippedHighWaterFor(recordingId), startPosition);
        long shipped = shipper.shipSegments(
                archiveDir, recordingId, priorShipped, candidateFloor, extent.segmentFileLength());
        if (shipped > priorShipped) {
            shipState.markShipped(recordingId, shipped);
        }
        long shippedHighWater = Math.max(shipState.shippedHighWaterFor(recordingId), shipped);
        long purgeTo = AeronArchive.segmentFileBasePosition(
                extent.startPosition(),
                Math.min(candidateFloor, shippedHighWater),
                extent.termBufferLength(),
                extent.segmentFileLength());
        if (purgeTo <= startPosition) {
            log.debug(
                    "purge deferred recordingId={}: shipped high-water {} not past start {}",
                    recordingId,
                    shippedHighWater,
                    startPosition);
            return false;
        }
        long segmentsDeleted = archive.purgeSegments(recordingId, purgeTo);
        if (segmentsReclaimedCounter != null) {
            segmentsReclaimedCounter.increment(segmentsDeleted);
        }
        log.info(
                "retention purge applied: recordingId={} purgeTo={} segmentsDeleted={}",
                recordingId,
                purgeTo,
                segmentsDeleted);
        return true;
    }

    /**
     * Ships the remainder of a stopped, fully-consumed recording and purges it whole
     * (descriptor + segments). Returns true when the recording was purged.
     */
    private boolean shipAndPurgeWholeRecording(AeronArchive archive, RecordingExtent extent) {
        long recordingId = extent.recordingId();
        long stopPosition = archive.getStopPosition(recordingId);
        if (stopPosition < 0L) {
            return false; // still recording — not safe to purge whole
        }
        long startPosition = archive.getStartPosition(recordingId);
        long priorShipped = Math.max(shipState.shippedHighWaterFor(recordingId), startPosition);
        long shipped = priorShipped;
        if (priorShipped < stopPosition) {
            shipped = shipper.shipSegments(
                    archiveDir, recordingId, priorShipped, Long.MAX_VALUE, extent.segmentFileLength());
            if (shipped > priorShipped) {
                shipState.markShipped(recordingId, shipped);
            }
        }
        if (Math.max(shipped, startPosition) < stopPosition) {
            log.debug(
                    "whole-recording purge deferred recordingId={}: shipped {} < stop {}",
                    recordingId,
                    shipped,
                    stopPosition);
            return false;
        }
        archive.purgeRecording(recordingId);
        log.info(
                "purged fully-consumed events recordingId={} (stopPosition={})", recordingId, stopPosition);
        return true;
    }

    private JdbcConsumerCursorProbe.ConsumerCursor probeConsumerCursor() {
        if (!config.hasJdbcCursorProbe()) {
            return null;
        }
        try (JdbcConsumerCursorProbe probe =
                new JdbcConsumerCursorProbe(
                        config.pgUrl(), config.pgUser(), config.pgPassword(), config.projectorId())) {
            return probe.minConsumerCursor().orElse(null);
        } catch (Exception ex) {
            log.warn("events purge skipped: JDBC cursor probe failed", ex);
            return null;
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

    /**
     * Purges snapshot recordings older than the {@code snapshotsToRetain} most recent snapshots.
     * Snapshots are grouped by log position so the consensus-module and service entries of one
     * snapshot are always purged (or retained) together. Returns true when any were purged.
     */
    private boolean purgeOldSnapshotRecordings(AeronArchive archive) {
        TreeMap<Long, List<Long>> recordingIdsBySnapshotPosition = new TreeMap<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    recordingIdsBySnapshotPosition
                            .computeIfAbsent(entry.logPosition, k -> new ArrayList<>())
                            .add(entry.recordingId);
                }
            }
        }
        int toPurge = recordingIdsBySnapshotPosition.size() - config.snapshotsToRetain();
        if (toPurge <= 0) {
            return false;
        }
        boolean purgedAny = false;
        Iterator<Map.Entry<Long, List<Long>>> oldestFirst =
                recordingIdsBySnapshotPosition.entrySet().iterator();
        for (int i = 0; i < toPurge && oldestFirst.hasNext(); i++) {
            Map.Entry<Long, List<Long>> snapshot = oldestFirst.next();
            for (long recordingId : snapshot.getValue()) {
                if (describeRecording(archive, recordingId) == null) {
                    continue; // already purged on a previous pass (RecordingLog entry stays valid)
                }
                try {
                    archive.purgeRecording(recordingId);
                    purgedAny = true;
                    log.info(
                            "purged old snapshot recordingId={} logPosition={}",
                            recordingId,
                            snapshot.getKey());
                } catch (Exception ex) {
                    log.warn("failed to purge old snapshot recordingId={}", recordingId, ex);
                }
            }
        }
        return purgedAny;
    }

    private long unshippedLagFor(AeronArchive archive, RecordingExtent extent) {
        long current = currentPosition(archive, extent.recordingId());
        long shipped = Math.max(
                shipState.shippedHighWaterFor(extent.recordingId()),
                archive.getStartPosition(extent.recordingId()));
        return Math.max(0L, current - shipped);
    }

    private static long currentPosition(AeronArchive archive, long recordingId) {
        long position = archive.getRecordingPosition(recordingId);
        if (position < 0L) {
            position = archive.getStopPosition(recordingId);
        }
        return Math.max(position, 0L);
    }

    /** Recording descriptor fields needed for segment math; positions are recording-relative bytes. */
    record RecordingExtent(
            long recordingId,
            long startPosition,
            long stopPosition,
            int segmentFileLength,
            int termBufferLength,
            int streamId) {}

    private static RecordingExtent describeRecording(AeronArchive archive, long recordingId) {
        List<RecordingExtent> out = new ArrayList<>(1);
        archive.listRecording(
                recordingId,
                (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp, startPosition,
                        stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                        sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                        out.add(new RecordingExtent(
                                recId, startPosition, stopPosition, segmentFileLength, termBufferLength, streamId)));
        return out.isEmpty() ? null : out.get(0);
    }

    private static List<RecordingExtent> listRecordingsForStream(AeronArchive archive, int streamId) {
        List<RecordingExtent> out = new ArrayList<>();
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp, startPosition,
                        stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                        sessionId, recordingStreamId, strippedChannel, originalChannel, sourceIdentity) -> {
                    if (recordingStreamId == streamId) {
                        out.add(new RecordingExtent(
                                recId,
                                startPosition,
                                stopPosition,
                                segmentFileLength,
                                termBufferLength,
                                recordingStreamId));
                    }
                });
        return out;
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
