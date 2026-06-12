package com.balh.oms.cluster.retention;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks off-host shipped high-water positions per recording id (local text state file). */
public final class ArchiveShipStateStore {

    private final File stateFile;
    private final Map<Long, Long> shippedHighWater = new ConcurrentHashMap<>();

    public ArchiveShipStateStore(File archiveDir) {
        this.stateFile = new File(archiveDir, "archive-ship-state.txt");
        load();
    }

    public long shippedHighWaterFor(long recordingId) {
        return shippedHighWater.getOrDefault(recordingId, 0L);
    }

    public void markShipped(long recordingId, long position) {
        shippedHighWater.merge(recordingId, position, Math::max);
        persist();
    }

    private void load() {
        if (!stateFile.exists()) {
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(stateFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int eq = line.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                try {
                    long recordingId = Long.parseLong(line.substring(0, eq).trim());
                    long position = Long.parseLong(line.substring(eq + 1).trim());
                    shippedHighWater.put(recordingId, position);
                } catch (NumberFormatException ignored) {
                    // skip corrupt lines
                }
            }
        } catch (IOException ex) {
            // start fresh
        }
    }

    private void persist() {
        try {
            File parent = stateFile.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("could not create dir: " + parent);
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(stateFile))) {
                writer.write("# recordingId=shippedHighWaterPosition\n");
                for (Map.Entry<Long, Long> e : shippedHighWater.entrySet()) {
                    writer.write(e.getKey() + "=" + e.getValue());
                    writer.newLine();
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("failed to persist archive ship state: " + stateFile, ex);
        }
    }
}
