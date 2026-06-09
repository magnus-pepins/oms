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
    void evaluateLagReading_caughtUp_returnsZeroExcess() {
        stubComparableCursors(5000L, 5000L, 1L, 1L);
        OmsVenueEgressLagPublisher.VenueEgressLagReading reading = publisher.evaluateLagReading();
        assertThat(reading.measurable()).isTrue();
        assertThat(reading.rawLagBytes()).isZero();
        assertThat(reading.excessLagBytes()).isZero();
    }

    @Test
    void evaluateLagReading_serialEgressBehind_returnsRawAsExcess() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);
        stubComparableCursors(9000L, 6000L, 2L, 2L);
        OmsVenueEgressLagPublisher.VenueEgressLagReading reading = publisher.evaluateLagReading();
        assertThat(reading.rawLagBytes()).isEqualTo(3000L);
        assertThat(reading.excessLagBytes()).isEqualTo(3000L);
        assertThat(publisher.computeLagBytes()).isEqualTo(3000L);
    }

    @Test
    void evaluateLagReading_pipelinedHealthyWindow_rawHighExcessZero() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(512);
        long floor = config.getVenue().getAdmissionGate().pipelinedFloorBytes(512);
        stubComparableCursors(300_000L, 50_000L, 387L, 387L);

        OmsVenueEgressLagPublisher.VenueEgressLagReading reading = publisher.evaluateLagReading();
        assertThat(reading.rawLagBytes()).isEqualTo(250_000L);
        assertThat(reading.pipelinedFloorBytes()).isEqualTo(floor);
        assertThat(reading.excessLagBytes()).isZero();
        assertThat(publisher.computeLagBytes()).isZero();
    }

    @Test
    void evaluateLagReading_pipelinedStuckBeyondFloor_positiveExcess() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(512);
        long floor = config.getVenue().getAdmissionGate().pipelinedFloorBytes(512);
        stubComparableCursors(400_000L, 50_000L, 387L, 387L);

        OmsVenueEgressLagPublisher.VenueEgressLagReading reading = publisher.evaluateLagReading();
        assertThat(reading.rawLagBytes()).isEqualTo(350_000L);
        assertThat(reading.excessLagBytes()).isEqualTo(350_000L - floor);
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
    void pollCursors_updatesGaugeSupplierWithExcessLag() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);
        stubComparableCursors(8000L, 7500L, 1L, 1L);
        publisher.pollCursors();
        assertThat(publisher.currentLagBytes()).isEqualTo(500L);
        assertThat(publisher.currentRawLagBytes()).isEqualTo(500L);
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
        config.getVenue().getAdmissionGate().setMissingEgressBlockedPolls(1);
        config.getVenue().getAdmissionGate().setMissingEgressGraceMs(0L);
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
    void egressMissingWithProjectorLive_hysteresisRequiresConsecutivePolls() {
        config.getVenue().getAdmissionGate().setMissingEgressBlockedPolls(3);
        config.getVenue().getAdmissionGate().setMissingEgressGraceMs(60_000L);
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(900L));

        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
        assertThat(publisher.computeHealthSnapshot().admissible()).isTrue();
        assertThat(publisher.computeHealthSnapshot().admissible()).isFalse();
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
    void serialEgressBehindBeyondMaxExcess_blocks() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);
        stubPositions(20_000L, 6_432L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("excess lag");
    }

    @Test
    void withinMaxExcess_allows() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);
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

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isTrue();
        assertThat(snapshot.shouldThrottle()).isFalse();
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
    void softThrottleBand_delaysWithoutBlocking() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(512);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);
        config.getVenue().getAdmissionGate().setThrottleExcessLagBytes(2048L);
        long floor = config.getVenue().getAdmissionGate().pipelinedFloorBytes(512);
        long raw = floor + 3000L;
        stubPositions(raw + 50_000L, 50_000L);
        stubRecordings(387L, 387L);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isTrue();
        assertThat(snapshot.shouldThrottle()).isTrue();
        assertThat(snapshot.throttleDelayNanos()).isPositive();
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
    void egressOnNewerRecording_blocksUntilProjectorCatchesUp() {
        stubPositions(9920L, 10L);
        stubRecordings(387L, 388L);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("newer recording");
    }

    @Test
    void projectorTrailingEgressOnSameRecording_blocks() {
        stubPositions(1952L, 2624L);
        stubRecordings(0L, 0L);

        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot snapshot =
                publisher.computeHealthSnapshot();
        assertThat(snapshot.admissible()).isFalse();
        assertThat(snapshot.blockDetail()).contains("projector trailing");
    }

    @Test
    void pollCursors_updatesHealthSnapshot() {
        config.getCluster().getVenueEgress().setVenueRouteMaxInFlight(1);
        config.getVenue().getAdmissionGate().setMissingEgressBlockedPolls(1);
        config.getVenue().getAdmissionGate().setMissingEgressGraceMs(0L);
        stubPositions(20_000L, 6_432L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);

        publisher.pollCursors();

        assertThat(publisher.currentHealthSnapshot().admissible()).isFalse();
    }

    @Test
    void hardBlockRawLagBytes_isFloorPlusMaxExcess() {
        OmsConfig.Venue.AdmissionGate gate = config.getVenue().getAdmissionGate();
        gate.setMaxLagBytes(4096L);
        assertThat(gate.hardBlockRawLagBytes(512))
                .isEqualTo(gate.pipelinedFloorBytes(512) + 4096L);
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
