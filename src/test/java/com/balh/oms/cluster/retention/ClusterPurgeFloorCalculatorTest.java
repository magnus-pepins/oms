package com.balh.oms.cluster.retention;

import io.aeron.cluster.RecordingLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterPurgeFloorCalculatorTest {

    private static final int CONSENSUS_MODULE_SERVICE_ID = -1;

    @Test
    void countsWholeSnapshots_notPerServiceEntries(@TempDir File clusterDir) {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true)) {
            // Each cluster snapshot writes one entry per service plus one for the consensus
            // module, all at the same log position.
            recordingLog.appendSnapshot(10L, 0L, 0L, 1_000L, 1L, 0);
            recordingLog.appendSnapshot(11L, 0L, 0L, 1_000L, 1L, CONSENSUS_MODULE_SERVICE_ID);
            recordingLog.appendSnapshot(12L, 0L, 0L, 2_000L, 2L, 0);
            recordingLog.appendSnapshot(13L, 0L, 0L, 2_000L, 2L, CONSENSUS_MODULE_SERVICE_ID);
            recordingLog.appendSnapshot(14L, 0L, 0L, 3_000L, 3L, 0);
            recordingLog.appendSnapshot(15L, 0L, 0L, 3_000L, 3L, CONSENSUS_MODULE_SERVICE_ID);
        }
        // retain=1 -> only the newest whole snapshot; floor at its log position.
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 1))
                .hasValue(3_000L);
        // retain=2 must count snapshots, not the 4 newest entries (which would give 2_000 anyway,
        // but retain=3 distinguishes: entry counting would yield 2_000, snapshot counting 1_000).
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 2))
                .hasValue(2_000L);
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 3))
                .hasValue(1_000L);
        // retaining more snapshots than exist keeps the earliest.
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 5))
                .hasValue(1_000L);
    }

    @Test
    void emptyWhenNoSnapshots(@TempDir File clusterDir) {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true)) {
            recordingLog.appendTerm(7L, 0L, 0L, 1L);
        }
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 3))
                .isEmpty();
    }

    @Test
    void emptyWhenRetainNonPositive(@TempDir File clusterDir) {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, true)) {
            recordingLog.appendSnapshot(10L, 0L, 0L, 1_000L, 1L, 0);
        }
        assertThat(ClusterPurgeFloorCalculator.earliestRetainedSnapshotLogPosition(clusterDir, 0))
                .isEmpty();
    }
}
