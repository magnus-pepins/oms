package com.balh.oms.fixegress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class OmsFixEgressCursorLagPublisherTest {

    private static final String EGRESS_ID = "oms-fix-egress-test";
    private static final int STREAM_ID = 2000;

    @Mock private OmsFixEgressCursorRepository cursorRepository;

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    void registersGaugeOnPostConstruct_withTags() {
        Clock fixed = Clock.fixed(Instant.parse("2026-05-12T10:00:00Z"), ZoneOffset.UTC);
        OmsFixEgressCursorLagPublisher publisher =
                new OmsFixEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);

        publisher.registerGauge();

        Gauge gauge =
                meterRegistry
                        .find(OmsFixEgressCursorLagPublisher.GAUGE_NAME)
                        .tag("egress_id", EGRESS_ID)
                        .tag("stream_id", Integer.toString(STREAM_ID))
                        .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(OmsFixEgressCursorLagPublisher.NO_DATA_LAG_SECONDS);
    }

    @Test
    void coldStart_returnsNoDataSentinel_evenAfterPollWithEmptyRow() {
        Clock fixed = Clock.fixed(Instant.parse("2026-05-12T10:00:00Z"), ZoneOffset.UTC);
        OmsFixEgressCursorLagPublisher publisher =
                new OmsFixEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.registerGauge();

        when(cursorRepository.findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID)))
                .thenReturn(Optional.empty());

        publisher.pollCursor();

        assertThat(publisher.currentLagSeconds())
                .isEqualTo(OmsFixEgressCursorLagPublisher.NO_DATA_LAG_SECONDS);
    }

    @Test
    void afterSuccessfulPoll_gaugeReportsLagInSeconds() {
        Instant lastAppliedAt = Instant.parse("2026-05-12T10:00:00Z");
        Instant now = lastAppliedAt.plus(Duration.ofMillis(750));
        Clock fixed = Clock.fixed(now, ZoneOffset.UTC);
        OmsFixEgressCursorLagPublisher publisher =
                new OmsFixEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.registerGauge();

        when(cursorRepository.findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID)))
                .thenReturn(Optional.of(lastAppliedAt));

        publisher.pollCursor();

        Gauge gauge =
                meterRegistry
                        .find(OmsFixEgressCursorLagPublisher.GAUGE_NAME)
                        .tag("egress_id", EGRESS_ID)
                        .gauge();
        assertThat(gauge.value()).isCloseTo(0.75, within(0.001));
    }

    @Test
    void clockEarlierThanCursor_clampsToZero_doesNotReportNegativeLag() {
        Instant lastAppliedAt = Instant.parse("2026-05-12T10:00:01Z");
        Instant now = Instant.parse("2026-05-12T10:00:00Z");
        Clock fixed = Clock.fixed(now, ZoneOffset.UTC);
        OmsFixEgressCursorLagPublisher publisher =
                new OmsFixEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.registerGauge();

        when(cursorRepository.findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID)))
                .thenReturn(Optional.of(lastAppliedAt));
        publisher.pollCursor();

        assertThat(publisher.currentLagSeconds()).isEqualTo(0.0);
    }

    @Test
    void pollFailure_keepsLastCachedValue() {
        Instant lastAppliedAt = Instant.parse("2026-05-12T10:00:00Z");
        Instant now = lastAppliedAt.plus(Duration.ofSeconds(2));
        Clock fixed = Clock.fixed(now, ZoneOffset.UTC);
        OmsFixEgressCursorLagPublisher publisher =
                new OmsFixEgressCursorLagPublisher(
                        cursorRepository, meterRegistry, fixed, EGRESS_ID, STREAM_ID);
        publisher.registerGauge();

        when(cursorRepository.findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID)))
                .thenReturn(Optional.of(lastAppliedAt));
        publisher.pollCursor();
        assertThat(publisher.currentLagSeconds()).isCloseTo(2.0, within(0.001));

        doThrow(new DataAccessResourceFailureException("postgres unreachable"))
                .when(cursorRepository)
                .findLastAppliedAt(eq(EGRESS_ID), eq(STREAM_ID));

        publisher.pollCursor();

        assertThat(publisher.currentLagSeconds()).isCloseTo(2.0, within(0.001));
    }
}
