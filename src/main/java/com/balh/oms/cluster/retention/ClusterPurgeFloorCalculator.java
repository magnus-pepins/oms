package com.balh.oms.cluster.retention;

import io.aeron.cluster.RecordingLog;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;

/** Computes the purge floor from RecordingLog snapshots, consumer cursors, and ship state. */
public final class ClusterPurgeFloorCalculator {

    private ClusterPurgeFloorCalculator() {}

    public static OptionalLong earliestRetainedSnapshotLogPosition(File clusterDir, int snapshotsToRetain) {
        if (snapshotsToRetain <= 0) {
            return OptionalLong.empty();
        }
        List<Long> positions = new ArrayList<>();
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
        positions.sort(Comparator.naturalOrder());
        if (positions.size() <= snapshotsToRetain) {
            return OptionalLong.of(positions.get(0));
        }
        return OptionalLong.of(positions.get(positions.size() - snapshotsToRetain));
    }

    public static long roundDownToSegmentBoundary(long position, long segmentFileLength) {
        if (segmentFileLength <= 0L) {
            return position;
        }
        long remainder = position % segmentFileLength;
        return position - remainder;
    }

    public static long computePurgeFloor(
            long snapshotFloor,
            long consumerFloor,
            long shippedFloor,
            long segmentFileLength) {
        long raw = Math.min(snapshotFloor, Math.min(consumerFloor, shippedFloor));
        return roundDownToSegmentBoundary(raw, segmentFileLength);
    }
}
