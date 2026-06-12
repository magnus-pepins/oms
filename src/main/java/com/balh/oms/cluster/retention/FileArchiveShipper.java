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
 * Copies detached archive segment files to an off-host ship root (local path, NFS mount, or
 * rclone-synced directory). S3 object-lock/WORM is achieved by shipping to an immutable bucket
 * via operator sync tooling against {@link ClusterRetentionConfig#shipRoot()}.
 */
public final class FileArchiveShipper {

    private static final Logger log = LoggerFactory.getLogger(FileArchiveShipper.class);

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
     * Ships segment files from {@code archiveDir} for the given recording up to {@code shipUpToPosition}
     * (exclusive of active tail). Returns the highest byte position confirmed shipped.
     */
    public long shipSegments(File archiveDir, long recordingId, long shipUpToPosition, long segmentFileLength) {
        if (shipUpToPosition <= 0L) {
            return 0L;
        }
        File recordingDir = new File(archiveDir, String.valueOf(recordingId));
        if (!recordingDir.isDirectory()) {
            log.warn("no archive recording dir for recordingId={} under {}", recordingId, archiveDir);
            return 0L;
        }
        Path destBase =
                shipRoot.toPath()
                        .resolve(clusterLabel)
                        .resolve(String.valueOf(recordingId))
                        .resolve(Instant.now().toString().replace(':', '-'));
        try {
            Files.createDirectories(destBase);
        } catch (IOException ex) {
            throw new IllegalStateException("failed to create ship dest " + destBase, ex);
        }

        long highestShipped = 0L;
        File[] segments = recordingDir.listFiles((dir, name) -> name.endsWith(".rec"));
        if (segments == null) {
            return 0L;
        }
        for (File segment : segments) {
            long segmentStart = parseSegmentStartPosition(segment.getName(), segmentFileLength);
            if (segmentStart < 0L || segmentStart >= shipUpToPosition) {
                continue;
            }
            Path dest = destBase.resolve(segment.getName());
            try {
                Files.copy(segment.toPath(), dest, StandardCopyOption.REPLACE_EXISTING);
                long segmentEnd = segmentStart + segmentFileLength;
                highestShipped = Math.max(highestShipped, Math.min(segmentEnd, shipUpToPosition));
                writeManifest(destBase, recordingId, segment.getName(), segmentStart, segmentEnd);
            } catch (IOException ex) {
                throw new IllegalStateException("failed to ship segment " + segment, ex);
            }
        }
        log.info(
                "shipped archive segments recordingId={} upTo={} highestShipped={} dest={}",
                recordingId,
                shipUpToPosition,
                highestShipped,
                destBase);
        return highestShipped;
    }

    /** Restores shipped segments from ship root back into archive dir (DR path). */
    public void restoreRecording(File archiveDir, long recordingId, Path shipManifestDir) throws IOException {
        File recordingDir = new File(archiveDir, String.valueOf(recordingId));
        if (!recordingDir.exists() && !recordingDir.mkdirs()) {
            throw new IOException("could not create recording dir " + recordingDir);
        }
        try (var paths = Files.walk(shipManifestDir)) {
            paths.filter(p -> p.toString().endsWith(".rec")).forEach(src -> {
                try {
                    Files.copy(src, recordingDir.toPath().resolve(src.getFileName()), StandardCopyOption.REPLACE_EXISTING);
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
     * Aeron segment file names encode the start position as the basename without extension.
     * Returns -1 if the name cannot be parsed.
     */
    static long parseSegmentStartPosition(String segmentFileName, long segmentFileLength) {
        String base = segmentFileName;
        if (base.endsWith(".rec")) {
            base = base.substring(0, base.length() - 4);
        }
        try {
            return Long.parseLong(base);
        } catch (NumberFormatException ex) {
            return -1L;
        }
    }
}
