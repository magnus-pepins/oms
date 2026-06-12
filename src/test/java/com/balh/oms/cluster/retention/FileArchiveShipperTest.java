package com.balh.oms.cluster.retention;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileArchiveShipperTest {

    private static final long SEGMENT_LENGTH = 1_024L;

    @Test
    void parsesFlatAeronSegmentFileNames() {
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-0.rec", 5L)).isEqualTo(0L);
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-262144.rec", 5L)).isEqualTo(262_144L);
        // other recording ids must not match (including prefix collisions)
        assertThat(FileArchiveShipper.parseSegmentBasePosition("50-0.rec", 5L)).isEqualTo(-1L);
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-0.rec", 50L)).isEqualTo(-1L);
        // tombstones, state files, and garbage are ignored
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-262144.rec.del", 5L)).isEqualTo(-1L);
        assertThat(FileArchiveShipper.parseSegmentBasePosition("archive-ship-state.txt", 5L)).isEqualTo(-1L);
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-abc.rec", 5L)).isEqualTo(-1L);
        assertThat(FileArchiveShipper.parseSegmentBasePosition("5-.rec", 5L)).isEqualTo(-1L);
    }

    @Test
    void shipsOnlySegmentsInRange_andWritesManifest(@TempDir Path tempDir) throws IOException {
        File archiveDir = Files.createDirectories(tempDir.resolve("archive")).toFile();
        writeSegment(archiveDir, "5-0.rec");
        writeSegment(archiveDir, "5-1024.rec");
        writeSegment(archiveDir, "5-2048.rec"); // active tail above shipUpTo
        writeSegment(archiveDir, "7-0.rec"); // different recording
        File shipRoot = tempDir.resolve("ship").toFile();
        FileArchiveShipper shipper = new FileArchiveShipper(shipRoot, "test");

        long shipped = shipper.shipSegments(archiveDir, 5L, 0L, 2 * SEGMENT_LENGTH, SEGMENT_LENGTH);

        assertThat(shipped).isEqualTo(2 * SEGMENT_LENGTH);
        List<String> shippedNames = regularFileNames(shipRoot.toPath());
        assertThat(shippedNames)
                .contains("5-0.rec", "5-1024.rec", "manifest.txt")
                .doesNotContain("5-2048.rec", "7-0.rec");
    }

    @Test
    void skipsSegmentsAlreadyShipped(@TempDir Path tempDir) throws IOException {
        File archiveDir = Files.createDirectories(tempDir.resolve("archive")).toFile();
        writeSegment(archiveDir, "5-0.rec");
        writeSegment(archiveDir, "5-1024.rec");
        File shipRoot = tempDir.resolve("ship").toFile();
        FileArchiveShipper shipper = new FileArchiveShipper(shipRoot, "test");

        long shipped = shipper.shipSegments(
                archiveDir, 5L, SEGMENT_LENGTH, 2 * SEGMENT_LENGTH, SEGMENT_LENGTH);

        assertThat(shipped).isEqualTo(2 * SEGMENT_LENGTH);
        assertThat(regularFileNames(shipRoot.toPath()))
                .contains("5-1024.rec")
                .doesNotContain("5-0.rec");
    }

    @Test
    void returnsShipFrom_whenNothingNewToShip(@TempDir Path tempDir) throws IOException {
        File archiveDir = Files.createDirectories(tempDir.resolve("archive")).toFile();
        File shipRoot = tempDir.resolve("ship").toFile();
        FileArchiveShipper shipper = new FileArchiveShipper(shipRoot, "test");

        assertThat(shipper.shipSegments(archiveDir, 5L, 1_024L, 2_048L, SEGMENT_LENGTH))
                .isEqualTo(1_024L);
        assertThat(shipper.shipSegments(archiveDir, 5L, 2_048L, 2_048L, SEGMENT_LENGTH))
                .isEqualTo(2_048L);
        assertThat(shipRoot.exists()).isFalse();
    }

    @Test
    void restoresShippedSegmentsIntoFlatArchiveDir(@TempDir Path tempDir) throws IOException {
        File archiveDir = Files.createDirectories(tempDir.resolve("archive")).toFile();
        writeSegment(archiveDir, "5-0.rec");
        File shipRoot = tempDir.resolve("ship").toFile();
        FileArchiveShipper shipper = new FileArchiveShipper(shipRoot, "test");
        shipper.shipSegments(archiveDir, 5L, 0L, SEGMENT_LENGTH, SEGMENT_LENGTH);

        File restoredDir = Files.createDirectories(tempDir.resolve("restored")).toFile();
        shipper.restoreRecording(restoredDir, 5L, shipRoot.toPath());

        assertThat(new File(restoredDir, "5-0.rec")).exists();
    }

    private static void writeSegment(File archiveDir, String name) throws IOException {
        Files.write(new File(archiveDir, name).toPath(), new byte[16]);
    }

    private static List<String> regularFileNames(Path root) throws IOException {
        if (!Files.exists(root)) {
            return List.of();
        }
        try (var stream = Files.walk(root)) {
            return stream.filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .toList();
        }
    }
}
