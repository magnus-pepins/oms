package com.balh.oms.cluster.retention;

import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.util.OptionalLong;
import java.util.TreeSet;

/** Computes purge floors from the cluster {@link RecordingLog} snapshot entries. */
public final class ClusterPurgeFloorCalculator {

    private ClusterPurgeFloorCalculator() {}

    /**
     * Log position of the earliest snapshot that must be retained, or empty when no snapshot
     * exists. Each cluster snapshot produces one {@link RecordingLog} entry per service plus one
     * for the consensus module, all sharing the same log position — entries are deduplicated by
     * log position so {@code snapshotsToRetain} counts whole snapshots, not entries.
     */
    public static OptionalLong earliestRetainedSnapshotLogPosition(File clusterDir, int snapshotsToRetain) {
        if (snapshotsToRetain <= 0) {
            return OptionalLong.empty();
        }
        TreeSet<Long> positions = new TreeSet<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid) {
                    positions.add(entry.logPosition);
                }
            }
        }
        if (positions.isEmpty()) {
            return OptionalLong.empty();
        }
        Long[] sorted = positions.toArray(new Long[0]);
        int index = Math.max(0, sorted.length - snapshotsToRetain);
        return OptionalLong.of(sorted[index]);
    }
}
