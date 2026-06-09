package com.balh.oms.cluster;

import io.aeron.archive.client.AeronArchive;

import java.util.List;
import java.util.Optional;

/**
 * Shared Aeron Archive events-recording helpers for {@code oms-postgres-projector} and
 * {@code oms-fix-egress}. Both consumers bootstrap from the events stream (2000) and can
 * hit empty tombstone recordings after a cluster wipe — e.g. recording id {@code 0} with
 * {@code stopPosition == startPosition == 0}. Archive rejects {@code replay(id, 0, ...)} on
 * such recordings ({@code highest recorded position=0}), which previously killed the replay
 * thread while Spring stayed up.
 */
public final class OmsClusterEventsRecordingSupport {

    private OmsClusterEventsRecordingSupport() {}

    /**
     * Effective upper bound for a recording at this instant. For a stopped recording it is the
     * finalized {@code stopPosition}; for an active recording it is the Archive's live write-head
     * position. Returns {@code startPosition} when neither is available (active recording with no
     * events yet).
     */
    public static long recordingUpperBound(
            AeronArchive archive, long recordingId, long startPosition, long stopPosition) {
        if (stopPosition != AeronArchive.NULL_POSITION) {
            return stopPosition;
        }
        long head = archive.getRecordingPosition(recordingId);
        if (head == AeronArchive.NULL_POSITION) {
            return startPosition;
        }
        return head;
    }

    /**
     * {@code true} when the recording contains no replayable bytes yet — empty closed tombstone
     * ({@code stopPosition == startPosition}) or an open tail with no writes.
     */
    public static boolean isEmptyRecording(
            AeronArchive archive, long recordingId, long startPosition, long stopPosition) {
        long upperBound = recordingUpperBound(archive, recordingId, startPosition, stopPosition);
        return isEmptyAtBounds(upperBound, startPosition);
    }

    /** Pure bounds check — package-private for unit tests. */
    static boolean isEmptyAtBounds(long upperBound, long startPosition) {
        return upperBound <= startPosition;
    }

    /**
     * On first-ever start, pick the lowest-id recording on the events stream that actually
     * contains data (or is a live non-empty tail), skipping empty tombstones such as id {@code 0}
     * after a cluster wipe.
     */
    public static Optional<BootstrapPick> pickBootstrapRecording(
            AeronArchive archive, List<BootstrapPick> sortedAscByRecordingId) {
        int skippedEmpty = 0;
        for (BootstrapPick candidate : sortedAscByRecordingId) {
            if (isEmptyRecording(
                    archive,
                    candidate.recordingId(),
                    candidate.startPosition(),
                    candidate.stopPosition())) {
                skippedEmpty++;
                continue;
            }
            return Optional.of(new BootstrapPick(
                    candidate.recordingId(),
                    candidate.startPosition(),
                    candidate.stopPosition(),
                    skippedEmpty));
        }
        return Optional.empty();
    }

    /**
     * {@link io.aeron.archive.client.ArchiveException} raised when opening replay at position
     * {@code 0} on an empty recording ({@code highest recorded position=0}).
     */
    public static boolean isEmptyRecordingReplayArchiveException(RuntimeException e) {
        if (!(e instanceof io.aeron.archive.client.ArchiveException)) {
            return false;
        }
        String message = e.getMessage();
        return message != null && message.contains("must be less than highest recorded position");
    }

    /**
     * Classifies replay-loop errors as recoverable OMS-cluster infrastructure failures (Aeron
     * MediaDriver / Archive unreachable after a cluster bounce) versus fatal cursor/recording
     * corruption ({@link IllegalStateException} from cursor guards — handled separately).
     */
    public static boolean isRecoverableClusterInfraError(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof io.aeron.exceptions.TimeoutException
                    || t instanceof io.aeron.exceptions.RegistrationException
                    || t instanceof org.agrona.concurrent.AgentTerminationException) {
                return true;
            }
        }
        return false;
    }

    /** Minimal recording identity for bootstrap selection. */
    public record BootstrapPick(long recordingId, long startPosition, long stopPosition, int skippedEmptyTombstones) {

        public BootstrapPick(long recordingId, long startPosition, long stopPosition) {
            this(recordingId, startPosition, stopPosition, 0);
        }
    }
}
