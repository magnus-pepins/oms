package com.balh.oms.cluster.retention;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Locale;

/**
 * Copies sealed archive segment files to an off-host ship root (local path, NFS mount, or
 * rclone-synced directory). S3 object-lock/WORM is achieved by shipping to an immutable bucket
 * via operator sync tooling against {@link ClusterRetentionConfig#shipRoot()}.
 *
 * <p>Aeron Archive stores every recording's segments flat in the archive directory, named
 * {@code <recordingId>-<segmentBasePosition>.rec}.
 */
public final class FileArchiveShipper {

    private static final Logger log = LoggerFactory.getLogger(FileArchiveShipper.class);

    private static final String SEGMENT_FILE_SUFFIX = ".rec";

    private final File shipRoot;
    private final String clusterLabel;

    public FileArchiveShipper(File shipRoot, String clusterLabel) {
        if (shipRoot == null || shipRoot.getPath().isBlank()) {
            throw new IllegalArgumentException("shipRoot is required");
        }
        this.shipRoot = shipRoot;
        this.clusterLabel = clusterLabel;
    }

    /**
     * Ships segment files of {@code recordingId} whose byte range overlaps
     * {@code [shipFromPosition, shipUpToPosition)}. Segments ending at or below
     * {@code shipFromPosition} are assumed already shipped and skipped. Returns the highest byte
     * position confirmed shipped (capped at {@code shipUpToPosition}), or {@code shipFromPosition}
     * when nothing new was shipped.
     */
    public long shipSegments(
            File archiveDir,
            long recordingId,
            long shipFromPosition,
            long shipUpToPosition,
            long segmentFileLength) {
        if (shipUpToPosition <= shipFromPosition) {
            return shipFromPosition;
        }
        File[] segments =
                archiveDir.listFiles((dir, name) -> parseSegmentBasePosition(name, recordingId) >= 0L);
        if (segments == null || segments.length == 0) {
            log.debug("no archive segment files for recordingId={} under {}", recordingId, archiveDir);
            return shipFromPosition;
        }
        Path destBase =
                shipRoot.toPath()
                        .resolve(clusterLabel)
                        .resolve(String.valueOf(recordingId))
                        .resolve(Instant.now().toString().replace(':', '-'));

        long highestShipped = shipFromPosition;
        int shippedCount = 0;
        for (File segment : segments) {
            long segmentBase = parseSegmentBasePosition(segment.getName(), recordingId);
            long segmentEnd = segmentBase + segmentFileLength;
            if (segmentBase >= shipUpToPosition || segmentEnd <= shipFromPosition) {
                continue;
            }
            try {
                if (shippedCount == 0) {
                    Files.createDirectories(destBase);
                }
                Path dest = destBase.resolve(segment.getName());
                Files.copy(segment.toPath(), dest, StandardCopyOption.REPLACE_EXISTING);
                highestShipped = Math.max(highestShipped, Math.min(segmentEnd, shipUpToPosition));
                writeManifest(destBase, recordingId, segment.getName(), segmentBase, segmentEnd);
                shippedCount++;
            } catch (IOException ex) {
                throw new IllegalStateException("failed to ship segment " + segment, ex);
            }
        }
        if (shippedCount > 0) {
            log.info(
                    "shipped {} archive segment(s) recordingId={} range=[{},{}) highestShipped={} dest={}",
                    shippedCount,
                    recordingId,
                    shipFromPosition,
                    shipUpToPosition,
                    highestShipped,
                    destBase);
        }
        return highestShipped;
    }

    /** Restores shipped segment files from a ship directory back into the flat archive dir (DR path). */
    public void restoreRecording(File archiveDir, long recordingId, Path shipManifestDir) throws IOException {
        if (!archiveDir.exists() && !archiveDir.mkdirs()) {
            throw new IOException("could not create archive dir " + archiveDir);
        }
        try (var paths = Files.walk(shipManifestDir)) {
            paths.filter(p -> parseSegmentBasePosition(p.getFileName().toString(), recordingId) >= 0L)
                    .forEach(src -> {
                        try {
                            Files.copy(
                                    src,
                                    archiveDir.toPath().resolve(src.getFileName().toString()),
                                    StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException ex) {
                            throw new IllegalStateException("restore failed for " + src, ex);
                        }
                    });
        }
    }

    private static void writeManifest(Path destBase, long recordingId, String segmentName, long start, long end)
            throws IOException {
        Path manifest = destBase.resolve("manifest.txt");
        String line = String.format(
                Locale.ROOT,
                "recordingId=%d segment=%s start=%d end=%d shippedAt=%s\n",
                recordingId,
                segmentName,
                start,
                end,
                Instant.now().toString());
        Files.writeString(manifest, line, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
    }

    /**
     * Parses {@code <recordingId>-<segmentBasePosition>.rec}; returns -1 when the name is not a
     * segment file of the given recording (including {@code .rec.del} tombstones).
     */
    static long parseSegmentBasePosition(String segmentFileName, long recordingId) {
        String prefix = recordingId + "-";
        if (!segmentFileName.startsWith(prefix) || !segmentFileName.endsWith(SEGMENT_FILE_SUFFIX)) {
            return -1L;
        }
        String positionPart =
                segmentFileName.substring(prefix.length(), segmentFileName.length() - SEGMENT_FILE_SUFFIX.length());
        if (positionPart.isEmpty()) {
            return -1L;
        }
        try {
            long position = Long.parseLong(positionPart);
            return position < 0L ? -1L : position;
        } catch (NumberFormatException ex) {
            return -1L;
        }
    }
}
