package com.balh.oms.cluster.admin;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;

import java.util.Locale;

/**
 * Read-only operator tool: dumps every recording known to the cluster's Aeron Archive, with
 * its stream id, start/stop positions, and stripped channel. Added 2026-05-23 to diagnose
 * the post-V55/V56 pop projector-stuck-on-recording-16 incident — the projector log showed
 * {@code recordingStop=-1, upperBound=0} on recording 16 but nothing else; without listing
 * all events recordings on the same stream there was no way to tell whether the cluster had
 * (a) reattached to the same recording or (b) moved on to a newer recordingId that the
 * projector's walk-forward should have picked up.
 *
 * <p>Same env-var contract as {@link OmsProjectorRebuildFromSnapshotTool}
 * ({@code OMS_AERON_MEDIA_DRIVER_DIR} or {@code OMS_AERON_DIR_BASE}). Connects as a plain
 * client; no consensus join, no cluster mutation.
 */
public final class OmsArchiveListRecordingsTool {

    public static final int EXIT_OK = 0;

    public static final int EXIT_FAILURE = 1;

    private OmsArchiveListRecordingsTool() {}

    public static void main(String[] args) {
        String aeronDir = OmsProjectorRebuildFromSnapshotTool.resolveAeronDirectory();
        System.out.printf(Locale.ROOT, "OmsArchiveListRecordingsTool starting (aeronDir=%s)%n", aeronDir);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                     .aeron(aeron)
                     .ownsAeronClient(false)
                     .controlRequestChannel(OmsProjectorRebuildFromSnapshotTool.DEFAULT_ARCHIVE_CONTROL_REQUEST)
                     .controlResponseChannel(OmsProjectorRebuildFromSnapshotTool.DEFAULT_ARCHIVE_CONTROL_RESPONSE))) {
            int[] count = new int[]{0};
            archive.listRecordings(0L, 4096, (controlSessionId,
                    correlationId,
                    recordingId,
                    startTimestamp,
                    stopTimestamp,
                    startPosition,
                    stopPosition,
                    initialTermId,
                    segmentFileLength,
                    termBufferLength,
                    mtuLength,
                    sessionId,
                    streamId,
                    strippedChannel,
                    originalChannel,
                    sourceIdentity) -> {
                long head = AeronArchive.NULL_POSITION;
                try {
                    head = archive.getRecordingPosition(recordingId);
                } catch (RuntimeException ignored) {
                    // recording not currently being written to — head stays NULL_POSITION.
                }
                System.out.printf(Locale.ROOT,
                        "rec=%d stream=%d startPos=%d stopPos=%d head=%d strippedChannel=%s%n",
                        recordingId, streamId, startPosition, stopPosition, head, strippedChannel);
                count[0]++;
            });
            System.out.printf(Locale.ROOT, "total recordings=%d%n", count[0]);
            System.exit(EXIT_OK);
        } catch (RuntimeException e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(EXIT_FAILURE);
        }
    }
}
