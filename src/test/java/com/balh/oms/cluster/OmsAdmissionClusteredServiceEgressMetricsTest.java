package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 4 Tier 2.5 phase D-investigate — the post-D-6 runbook flagged "per-admit egress publish
 * is the wall" as a falsifiable hypothesis. To run that falsifier on Pop! we need per-emit
 * timers + back-pressure counters in {@link OmsAdmissionClusteredService}. This test asserts
 * the meter contract:
 *
 * <ol>
 *   <li>One {@code applyAcceptOrder} accept publishes <em>two</em> messages — one
 *       {@link OrderAdmittedEvent} via {@code eventsPublication.offer} (projector consumes) and
 *       one {@link OrderAcceptedEvent} via {@code session.offer} (ingress client demuxes).
 *       Both timers should record exactly one sample per accept; both back-pressure counters
 *       should remain at zero when the publication mocks return success on the first offer.</li>
 *   <li>When the events publication returns {@code BACK_PRESSURED} (a small negative integer)
 *       for the first {@code N} retries before succeeding, the back-pressure counter records
 *       exactly {@code N} ticks — confirming the busy-wait counter wiring.</li>
 *   <li>Duplicate-key admits (which short-circuit before {@code emitAdmitted} but still publish
 *       a duplicate {@code OrderAcceptedEvent} via {@code session.offer}) record exactly one
 *       session sample and zero new events samples — used to validate per-event-kind tagging
 *       once the meters land in production.</li>
 * </ol>
 */
class OmsAdmissionClusteredServiceEgressMetricsTest {

    private static final long ANY_TIMESTAMP_MS = 1_700_000_000_999L;

    private MeterRegistry registry;
    private OmsAdmissionClusteredService service;
    private ClientSession session;
    private ExclusivePublication eventsPublicationMock;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new OmsAdmissionClusteredService(registry);

        session = mock(ClientSession.class);
        when(session.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);

        eventsPublicationMock = mock(ExclusivePublication.class);
        when(eventsPublicationMock.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);

        Aeron aeronMock = mock(Aeron.class);
        when(aeronMock.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(eventsPublicationMock);

        Cluster clusterMock = mock(Cluster.class);
        when(clusterMock.role()).thenReturn(Cluster.Role.LEADER);
        when(clusterMock.aeron()).thenReturn(aeronMock);
        service.onStart(clusterMock, /* snapshotImage = */ null);
    }

    @Test
    void freshAccept_recordsOneSampleOnEachPublishTimer_withZeroBackPressure() {
        deliverAccept(sampleAccept(1L, "acct", "idem", new UUID(0L, 1L)));

        assertThat(eventsAdmittedTimer().count())
                .as("eventsPublication.offer fires exactly once per fresh admit")
                .isEqualTo(1);
        assertThat(sessionAcceptedTimer().count())
                .as("session.offer fires exactly once per accept (fresh or duplicate)")
                .isEqualTo(1);
        assertThat(eventsAdmittedBackPressureCount())
                .as("offers returned success on first try; no Thread.yield ticks expected")
                .isZero();
        assertThat(sessionAcceptedBackPressureCount()).isZero();

        // Sanity: the recorded times are non-negative finite numbers (Timer.record clamps to
        // monotone wall time; in tests the duration is sub-millisecond).
        assertThat(eventsAdmittedTimer().totalTime(TimeUnit.NANOSECONDS)).isGreaterThanOrEqualTo(0);
        assertThat(sessionAcceptedTimer().totalTime(TimeUnit.NANOSECONDS)).isGreaterThanOrEqualTo(0);
    }

    @Test
    void backPressureOnEventsPublication_isCountedOncePerThreadYieldTick() {
        // Make eventsPublication.offer return BACK_PRESSURED (-2) for the first 3 calls, then
        // succeed on the 4th — exercising the busy-wait + counter increment path.
        AtomicInteger calls = new AtomicInteger();
        when(eventsPublicationMock.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(inv -> {
                    int n = calls.incrementAndGet();
                    return (n <= 3) ? io.aeron.Publication.BACK_PRESSURED : 1L;
                });

        deliverAccept(sampleAccept(2L, "acct-b", "idem-b", new UUID(0L, 2L)));

        assertThat(eventsAdmittedTimer().count())
                .as("the timer records once per emit regardless of how many busy-wait ticks happened")
                .isEqualTo(1);
        assertThat(eventsAdmittedBackPressureCount())
                .as("counter incremented once per BACK_PRESSURED Thread.yield iteration")
                .isEqualTo(3);
        assertThat(sessionAcceptedBackPressureCount()).isZero();
    }

    @Test
    void duplicateAdmit_recordsSessionEgressSampleButNotEventsAdmittedSample() {
        // First admit — primes both timers.
        deliverAccept(sampleAccept(10L, "acct", "idem", new UUID(0L, 10L)));
        // Second admit on the same idempotency key — short-circuits before emitAdmitted but still
        // emits an OrderAcceptedEvent with duplicate=true on the session.
        deliverAccept(sampleAccept(11L, "acct", "idem", new UUID(0L, 11L)));

        assertThat(eventsAdmittedTimer().count())
                .as("only the first (fresh) admit publishes OrderAdmittedEvent; duplicates short-circuit")
                .isEqualTo(1);
        assertThat(sessionAcceptedTimer().count())
                .as("both fresh and duplicate admits publish an OrderAcceptedEvent on the session")
                .isEqualTo(2);
    }

    private void deliverAccept(AcceptOrderCommand cmd) {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buf, 0);
        service.onSessionMessage(session, ANY_TIMESTAMP_MS, buf, 0, written, /* header = */ null);
    }

    private static AcceptOrderCommand sampleAccept(
            long correlationId, String accountId, String idemKey, UUID orderId) {
        return new AcceptOrderCommand(
                correlationId,
                orderId,
                /* clientTimestampNanos = */ 0L,
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId,
                idemKey,
                "hash-" + accountId,
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }

    private io.micrometer.core.instrument.Timer eventsAdmittedTimer() {
        return registry.find("oms.cluster.service.events_offer_seconds")
                .tag("event_kind", "admitted").timer();
    }

    private io.micrometer.core.instrument.Timer sessionAcceptedTimer() {
        return registry.find("oms.cluster.service.session_offer_seconds")
                .tag("event_kind", "accepted").timer();
    }

    private double eventsAdmittedBackPressureCount() {
        return registry.find("oms.cluster.service.events_offer_back_pressure_total")
                .tag("event_kind", "admitted").counter().count();
    }

    private double sessionAcceptedBackPressureCount() {
        return registry.find("oms.cluster.service.session_offer_back_pressure_total")
                .tag("event_kind", "accepted").counter().count();
    }
}
