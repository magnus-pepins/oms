package com.balh.oms.cluster;

import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OmsClusterEventsRecordingSupportTest {

    @Mock
    private AeronArchive archive;

    @Test
    void isEmptyAtBounds_emptyClosedTombstone() {
        assertThat(OmsClusterEventsRecordingSupport.isEmptyAtBounds(0L, 0L)).isTrue();
    }

    @Test
    void isEmptyAtBounds_liveTailWithData() {
        assertThat(OmsClusterEventsRecordingSupport.isEmptyAtBounds(1920L, 0L)).isFalse();
    }

    @Test
    void isEmptyAtBounds_caughtUpOnNonEmptyRecording() {
        assertThat(OmsClusterEventsRecordingSupport.isEmptyAtBounds(1920L, 0L)).isFalse();
    }

    @Test
    void pickBootstrapRecording_skipsEmptyTombstonesAndPicksFirstWithData() {
        when(archive.getRecordingPosition(387L)).thenReturn(1920L);

        var pick = OmsClusterEventsRecordingSupport.pickBootstrapRecording(
                archive,
                List.of(
                        new OmsClusterEventsRecordingSupport.BootstrapPick(0L, 0L, 0L),
                        new OmsClusterEventsRecordingSupport.BootstrapPick(8L, 0L, 0L),
                        new OmsClusterEventsRecordingSupport.BootstrapPick(387L, 0L, AeronArchive.NULL_POSITION)));

        assertThat(pick).isPresent();
        assertThat(pick.get().recordingId()).isEqualTo(387L);
        assertThat(pick.get().skippedEmptyTombstones()).isEqualTo(2);
    }

    @Test
    void pickBootstrapRecording_emptyWhenAllTombstones() {
        var pick = OmsClusterEventsRecordingSupport.pickBootstrapRecording(
                archive,
                List.of(
                        new OmsClusterEventsRecordingSupport.BootstrapPick(0L, 0L, 0L),
                        new OmsClusterEventsRecordingSupport.BootstrapPick(8L, 0L, 0L)));

        assertThat(pick).isEmpty();
    }

    @Test
    void isEmptyRecordingReplayArchiveException_matchesPopIncidentMessage() {
        var ex = new io.aeron.archive.client.ArchiveException(
                "ERROR - response for correlationId=3497, error: requested replay start position=0"
                        + " must be less than highest recorded position=0 for recording 0");
        assertThat(OmsClusterEventsRecordingSupport.isEmptyRecordingReplayArchiveException(ex)).isTrue();
    }

    @Test
    void isEmptyRecordingReplayArchiveException_rejectsUnrelatedArchiveException() {
        var ex = new io.aeron.archive.client.ArchiveException("recording not found");
        assertThat(OmsClusterEventsRecordingSupport.isEmptyRecordingReplayArchiveException(ex)).isFalse();
    }

    @Test
    void isEmptyRecordingReplayArchiveException_rejectsNonArchiveRuntimeException() {
        assertThat(OmsClusterEventsRecordingSupport.isEmptyRecordingReplayArchiveException(
                        new IllegalStateException("must be less than highest recorded position")))
                .isFalse();
    }
}
