package com.balh.oms.ingress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.projector.AeronProjectorCursorRepository;
import com.balh.oms.projector.OmsPostgresProjector;
import com.balh.oms.venueegress.OmsVenueEgressCursorRepository;
import com.balh.oms.venueegress.OmsVenueEgressService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OmsVenueEgressLagPublisherTest {

    private static final int STREAM = OmsClusterWireFormat.EVENTS_STREAM_ID;

    private OmsConfig config;
    private AeronProjectorCursorRepository projectorCursor;
    private OmsVenueEgressCursorRepository venueEgressCursor;
    private OmsVenueEgressLagPublisher publisher;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        projectorCursor = mock(AeronProjectorCursorRepository.class);
        venueEgressCursor = mock(OmsVenueEgressCursorRepository.class);
        publisher =
                new OmsVenueEgressLagPublisher(
                        config,
                        projectorCursor,
                        venueEgressCursor,
                        new SimpleMeterRegistry(),
                        OmsVenueEgressService.EGRESS_ID,
                        STREAM);
    }

    @Test
    void computeLagBytes_missingEgress_returnsSentinel() {
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(1000L));
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());

        assertThat(publisher.computeLagBytes()).isEqualTo((long) OmsVenueEgressLagPublisher.NO_DATA_LAG_BYTES);
    }

    @Test
    void computeLagBytes_caughtUp_returnsZero() {
        stubComparableCursors(5000L, 5000L, 1L, 1L);
        assertThat(publisher.computeLagBytes()).isZero();
    }

    @Test
    void computeLagBytes_egressBehind_returnsDelta() {
        stubComparableCursors(9000L, 6000L, 2L, 2L);
        assertThat(publisher.computeLagBytes()).isEqualTo(3000L);
    }

    @Test
    void computeLagBytes_egressOnOlderRecording_returnsMaxValue() {
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(100L));
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.of(50L));
        when(projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(
                        Optional.of(AeronProjectorCursorRepository.RecordedCursor.of(5L, 100L)));
        when(venueEgressCursor.findLastAppliedCursor(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(
                        Optional.of(OmsVenueEgressCursorRepository.RecordedCursor.of(3L, 50L)));

        assertThat(publisher.computeLagBytes()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void pollCursors_updatesGaugeSupplier() {
        stubComparableCursors(8000L, 7500L, 1L, 1L);
        publisher.pollCursors();
        assertThat(publisher.currentLagBytes()).isEqualTo(500L);
    }

    @Test
    void disabledGate_healthSnapshotAlwaysAllows() {
        config.getVenue().getAdmissionGate().setEnabled(false);
        stubPositions(20_000L, 6_432L);
        stubRecordings(387L, 387L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void freshStack_bothCursorsAbsent_allows() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.empty());

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void egressNeverApplied_projectorLive_blocks() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(512L));

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("venue egress cursor absent");
    }

    @Test
    void freshStack_projectorEmpty_allows() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.of(0L));
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.empty());

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void caughtUp_sameRecording_allows() {
        stubPositions(9920L, 9920L);
        stubRecordings(387L, 387L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void egressBehindBeyondMaxLag_blocks() {
        stubPositions(20_000L, 6_432L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("exceeds max");
    }

    @Test
    void withinMaxLag_allows() {
        stubPositions(9920L, 9900L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void pipelinedHealthyLag_withinInFlightBudget_allows() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(512);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);
        stubPositions(300_000L, 50_000L);
        stubRecordings(387L, 387L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void pipelinedEgressStuckBeyondInFlightBudget_blocks() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(512);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);
        stubPositions(400_000L, 50_000L);
        stubRecordings(387L, 387L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isFalse();
    }

    @Test
    void egressOnOlderRecording_blocks_regardlessOfPosition() {
        stubPositions(100L, 999_999L);
        stubRecordings(387L, 386L);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("older recording");
    }

    @Test
    void egressOnNewerRecording_allows() {
        stubPositions(9920L, 10L);
        stubRecordings(387L, 388L);

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
    }

    @Test
    void pollCursors_updatesHealthSnapshot() {
        stubPositions(20_000L, 6_432L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);

        publisher.pollCursors();

        assertThat(publisher.currentHealthSnapshot().admissible()).isFalse();
    }

    private void stubPositions(long projectorPos, long egressPos) {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.of(egressPos));
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(projectorPos));
    }

    private void stubRecordings(long projectorRec, long egressRec) {
        when(projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(Optional.of(AeronProjectorCursorRepository.RecordedCursor.of(projectorRec, 0L)));
        when(venueEgressCursor.findLastAppliedCursor(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(Optional.of(OmsVenueEgressCursorRepository.RecordedCursor.of(egressRec, 0L)));
    }

    private void stubComparableCursors(
            long projectorPos, long egressPos, long recordingId) {
        stubComparableCursors(projectorPos, egressPos, recordingId, recordingId);
    }

    private void stubComparableCursors(
            long projectorPos, long egressPos, long projectorRecording, long egressRecording) {
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(projectorPos));
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.of(egressPos));
        when(projectorCursor.findLastAppliedCursor(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(
                        Optional.of(
                                AeronProjectorCursorRepository.RecordedCursor.of(
                                        projectorRecording, projectorPos)));
        when(venueEgressCursor.findLastAppliedCursor(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(
                        Optional.of(
                                OmsVenueEgressCursorRepository.RecordedCursor.of(
                                        egressRecording, egressPos)));
    }
}
