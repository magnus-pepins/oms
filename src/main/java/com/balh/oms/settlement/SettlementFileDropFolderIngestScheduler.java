package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Optional poll of a host directory for {@code .json} broker confirm files. Same ingest semantics as
 * {@link SettlementFileIngestRouter}; processed files are moved under {@code .oms-done/} or
 * {@code .oms-failed/} under the watch directory. SFTP/S3 are out of scope here — copy files into this folder
 * with rclone or a broker sidecar (see docs/broker-eod-file-contract.md).
 */
@Component
public class SettlementFileDropFolderIngestScheduler {

    private static final Logger log = LoggerFactory.getLogger(SettlementFileDropFolderIngestScheduler.class);

    private static final String SOURCE_TAG = "drop-folder";
    private static final String SUB_DONE = ".oms-done";
    private static final String SUB_FAILED = ".oms-failed";

    private final OmsConfig config;
    private final SettlementFileIngestRouter settlementFileIngestRouter;

    public SettlementFileDropFolderIngestScheduler(
            OmsConfig config, SettlementFileIngestRouter settlementFileIngestRouter) {
        this.config = config;
        this.settlementFileIngestRouter = settlementFileIngestRouter;
    }

    @Scheduled(fixedDelayString = "${oms.settlement.file-import-drop-folder-poll-interval-ms:30000}")
    public void pollDropFolder() {
        var st = config.getSettlement();
        if (!st.isFileImportDropFolderEnabled()) {
            return;
        }
        String dir = st.getFileImportDropFolderPath();
        if (dir == null || dir.isBlank()) {
            log.warn("file-import drop folder enabled but path is blank — skipping poll");
            return;
        }
        Path root = Path.of(dir.trim()).toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            log.warn("file-import drop folder is not a directory: {}", root);
            return;
        }
        Path doneDir = root.resolve(SUB_DONE);
        Path failedDir = root.resolve(SUB_FAILED);
        try {
            Files.createDirectories(doneDir);
            Files.createDirectories(failedDir);
        } catch (IOException e) {
            log.warn("Could not create archive subdirs under {}", root, e);
            return;
        }
        int max = st.getFileImportDropFolderMaxFilesPerPoll();
        int processed = 0;
        try (Stream<Path> stream = Files.list(root)) {
            List<Path> candidates =
                    stream.filter(p -> Files.isRegularFile(p))
                            .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".json"))
                            .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                            .limit(max)
                            .toList();
            for (Path file : candidates) {
                if (processed >= max) {
                    break;
                }
                processed++;
                ingestOne(root, doneDir, failedDir, file);
            }
        } catch (IOException e) {
            log.warn("Drop folder poll failed for {}", root, e);
        }
    }

    private void ingestOne(Path root, Path doneDir, Path failedDir, Path file) {
        String name = file.getFileName().toString();
        try {
            byte[] bytes = Files.readAllBytes(file);
            SettlementFileIngestRouter.RoutedResult r =
                    settlementFileIngestRouter.ingest(SOURCE_TAG, name, bytes);
            Path destDir = isFailedStatus(r.status()) ? failedDir : doneDir;
            Path dest = uniqueDest(destDir, name);
            Files.move(file, dest, StandardCopyOption.REPLACE_EXISTING);
            log.info(
                    "Drop-folder ingest {} format={} batchId={} status={} duplicate={}",
                    name,
                    r.format(),
                    r.batchId(),
                    r.status(),
                    r.duplicate());
        } catch (IllegalArgumentException e) {
            log.warn("Drop-folder ingest rejected {}: {}", name, e.getMessage());
            moveQuietly(file, failedDir, name);
        } catch (Exception e) {
            log.warn("Drop-folder ingest failed for {}", name, e);
            moveQuietly(file, failedDir, name);
        }
    }

    private static boolean isFailedStatus(String status) {
        return "failed".equals(status) || "rejected".equals(status);
    }

    private static Path uniqueDest(Path dir, String originalName) {
        Path candidate = dir.resolve(originalName);
        if (!Files.exists(candidate)) {
            return candidate;
        }
        String base = originalName;
        String ext = "";
        int dot = originalName.lastIndexOf('.');
        if (dot > 0) {
            base = originalName.substring(0, dot);
            ext = originalName.substring(dot);
        }
        return dir.resolve(base + "-" + System.currentTimeMillis() + ext);
    }

    private static void moveQuietly(Path file, Path destDir, String name) {
        try {
            Files.move(file, uniqueDest(destDir, name), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            log.error("Could not archive failed drop-folder file {}", file, ex);
        }
    }
}
