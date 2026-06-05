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
import org.springframework.http.HttpStatus;

import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the venue-egress circuit breaker added after the 2026-06-03 incident where the OMS
 * kept admitting {@code PREDMKT/*} orders while {@code oms-venue-egress} was wedged ~3 500 events
 * behind. Confirms the gate trips on a behind / dead / older-recording egress, never gates equities,
 * and never gates a caught-up venue.
 */
class VenueAdmissionGateTest {

    private static final int STREAM = OmsClusterWireFormat.EVENTS_STREAM_ID;
    private static final String VENUE_SYMBOL = "PREDMKT-TEST-1";

    private OmsConfig config;
    private AeronProjectorCursorRepository projectorCursor;
    private OmsVenueEgressCursorRepository venueEgressCursor;
    private VenueAdmissionGate gate;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        projectorCursor = mock(AeronProjectorCursorRepository.class);
        venueEgressCursor = mock(OmsVenueEgressCursorRepository.class);
        gate = new VenueAdmissionGate(config, projectorCursor, venueEgressCursor, new SimpleMeterRegistry());
    }

    @Test
    void disabledGate_neverQueries_neverThrows() {
        config.getVenue().getAdmissionGate().setEnabled(false);

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
        verifyNoInteractions(projectorCursor, venueEgressCursor);
    }

    @Test
    void equitySymbol_isNotVenueRouted_neverQueries() {
        assertThatCode(() -> gate.assertVenueAdmissible("AAPL")).doesNotThrowAnyException();
        verifyNoInteractions(projectorCursor, venueEgressCursor);
    }

    @Test
    void freshStack_bothCursorsAbsent_allows() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.empty());

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
    }

    @Test
    void egressNeverApplied_projectorLive_trips503() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.empty());
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.of(512L));

        assertThatThrownBy(() -> gate.assertVenueAdmissible(VENUE_SYMBOL))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo(VenueAdmissionGate.REJECT_CODE);
                });
    }

    @Test
    void freshStack_projectorEmpty_allows() {
        when(venueEgressCursor.findLastAppliedPosition(OmsVenueEgressService.EGRESS_ID, STREAM))
                .thenReturn(OptionalLong.of(0L));
        when(projectorCursor.findLastAppliedPosition(OmsPostgresProjector.PROJECTOR_ID, STREAM))
                .thenReturn(OptionalLong.empty());

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
    }

    @Test
    void caughtUp_sameRecording_allows() {
        stubPositions(/* projector = */ 9920L, /* egress = */ 9920L);
        stubRecordings(/* projectorRec = */ 387L, /* egressRec = */ 387L);

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
    }

    @Test
    void egressBehindBeyondMaxLag_trips503() {
        // Wedged-egress shape: the projector keeps applying new admits while the egress is stuck, so
        // the lag grows unbounded and blows past the threshold (here projector 20 000, egress 6 432).
        stubPositions(/* projector = */ 20_000L, /* egress = */ 6_432L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);

        assertThatThrownBy(() -> gate.assertVenueAdmissible(VENUE_SYMBOL))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo(VenueAdmissionGate.REJECT_CODE);
                });
    }

    @Test
    void withinMaxLag_allows() {
        stubPositions(/* projector = */ 9920L, /* egress = */ 9900L);
        stubRecordings(387L, 387L);
        config.getVenue().getAdmissionGate().setMaxLagBytes(4096L);

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
    }

    @Test
    void egressOnOlderRecording_trips503_regardlessOfPosition() {
        // Egress position numerically ahead but on an older Aeron Archive recording → definitively behind.
        stubPositions(/* projector = */ 100L, /* egress = */ 999_999L);
        stubRecordings(/* projectorRec = */ 387L, /* egressRec = */ 386L);

        assertThatThrownBy(() -> gate.assertVenueAdmissible(VENUE_SYMBOL))
                .isInstanceOf(ClusterAdmissionException.class);
    }

    @Test
    void egressOnNewerRecording_allows() {
        stubPositions(/* projector = */ 9920L, /* egress = */ 10L);
        stubRecordings(/* projectorRec = */ 387L, /* egressRec = */ 388L);

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
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
}
