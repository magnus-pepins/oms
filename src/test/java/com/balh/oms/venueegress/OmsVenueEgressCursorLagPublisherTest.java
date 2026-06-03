package com.balh.oms.venueegress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class OmsVenueEgressCursorLagPublisherTest {

    private static final String EGRESS_ID = "oms-venue-egress-test";
    private static final int STREAM_ID = 2000;

    @Mock private OmsVenueEgressCursorRepository cursorRepository;

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    void prePoll_returnsSentinel() {
        Clock fixed = Clock.fixed(Instant.parse("2026-06-03T10:00:00Z"), ZoneOffset.UTC);
        OmsVenueEgressCursorLagPublisher publisher =
                new OmsVenueEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        assertThat(publisher.currentLagSeconds()).isEqualTo(OmsVenueEgressCursorLagPublisher.NO_DATA_LAG_SECONDS);
    }

    @Test
    void afterPoll_returnsElapsedSeconds() {
        Instant applied = Instant.parse("2026-06-03T09:59:00Z");
        Clock fixed = Clock.fixed(Instant.parse("2026-06-03T10:00:00Z"), ZoneOffset.UTC);
        when(cursorRepository.findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID)))
                .thenReturn(Optional.of(applied));
        OmsVenueEgressCursorLagPublisher publisher =
                new OmsVenueEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.pollCursor();
        assertThat(publisher.currentLagSeconds()).isEqualTo(60.0);
    }

    @Test
    void registersGaugeOnPostConstruct() {
        Clock fixed = Clock.fixed(Instant.parse("2026-06-03T10:00:00Z"), ZoneOffset.UTC);
        OmsVenueEgressCursorLagPublisher publisher =
                new OmsVenueEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.registerGauge();
        Gauge gauge =
                meterRegistry
                        .find(OmsVenueEgressCursorLagPublisher.GAUGE_NAME)
                        .tag("egress_id", EGRESS_ID)
                        .gauge();
        assertThat(gauge).isNotNull();
    }
}
