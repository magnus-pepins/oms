package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the venue-egress circuit breaker: hot path reads only the cached snapshot from
 * {@link OmsVenueEgressLagPublisher} (no per-request Postgres). Health verdict logic is covered in
 * {@link OmsVenueEgressLagPublisherTest}.
 */
class VenueAdmissionGateTest {

    private static final String VENUE_SYMBOL = "PREDMKT-TEST-1";

    private OmsConfig config;
    private OmsVenueEgressLagPublisher lagPublisher;
    private VenueAdmissionGate gate;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        lagPublisher = mock(OmsVenueEgressLagPublisher.class);
        gate = new VenueAdmissionGate(config, lagPublisher, new SimpleMeterRegistry());
    }

    @Test
    void disabledGate_neverReadsSnapshot_neverThrows() {
        config.getVenue().getAdmissionGate().setEnabled(false);

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
        verifyNoInteractions(lagPublisher);
    }

    @Test
    void equitySymbol_isNotVenueRouted_neverReadsSnapshot() {
        assertThatCode(() -> gate.assertVenueAdmissible("AAPL")).doesNotThrowAnyException();
        verifyNoInteractions(lagPublisher);
    }

    @Test
    void healthySnapshot_allows() {
        when(lagPublisher.currentHealthSnapshot())
                .thenReturn(OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot.allow());

        assertThatCode(() -> gate.assertVenueAdmissible(VENUE_SYMBOL)).doesNotThrowAnyException();
    }

    @Test
    void blockedSnapshot_trips503() {
        when(lagPublisher.currentHealthSnapshot())
                .thenReturn(
                        OmsVenueEgressLagPublisher.VenueEgressHealthSnapshot.block(
                                "venue egress lag 13568 bytes exceeds max 4096"));

        assertThatThrownBy(() -> gate.assertVenueAdmissible(VENUE_SYMBOL))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo(VenueAdmissionGate.REJECT_CODE);
                });
    }
}
