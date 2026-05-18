package com.balh.oms.fixegress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.fix.FixNewOrderSingleBuilder;
import com.balh.oms.fix.FixOrderCancelReplaceRequestBuilder;
import com.balh.oms.fix.FixOrderCancelRequestBuilder;
import com.balh.oms.fix.FixOutboundSessionSend;
import com.balh.oms.observability.metrics.OmsPipelineMeterNames;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.OrderCancelReplaceRequest;
import quickfix.fix44.OrderCancelRequest;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit-level coverage for {@link OmsFixEgressService#applyAdmittedEvent}: the cursor advance
 * (slice 3b-1) and the Phase 4j cluster-admit-to-NOS Timer recorded after a successful FIX send.
 *
 * <p>Stubs the FIX builder + send so we never stand up QuickFIX, and pins {@link Clock} so the
 * Timer's recorded duration is exactly {@code now - ev.acceptedAtMillis()} without flake.
 */
@ExtendWith(MockitoExtension.class)
class OmsFixEgressServiceTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long NOW_MS = ACCEPTED_AT_MS + 17L;
    private static final long FRAGMENT_POSITION = 4096L;

    @Mock private OmsFixEgressCursorRepository cursorRepository;
    @Mock private FixNewOrderSingleBuilder builder;
    @Mock private FixOutboundSessionSend send;

    private MeterRegistry meterRegistry;
    private OmsConfig config;
    private OmsFixEgressService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        config = new OmsConfig();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
        service = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                pinned,
                builder,
                /* orderCancelRequestBuilder = */ null,
                /* orderCancelReplaceRequestBuilder = */ null,
                send);
        // applyAdmittedEvent's send loop checks running.get(); init() sets it true via PostConstruct,
        // but we skip init() so the replay thread doesn't spin up. Flip the flag directly so the
        // synchronous send-and-record path under test executes the way it would in production.
        flipRunning(service, true);
    }

    private static void flipRunning(OmsFixEgressService svc, boolean value) {
        try {
            Field f = OmsFixEgressService.class.getDeclaredField("running");
            f.setAccessible(true);
            ((AtomicBoolean) f.get(svc)).set(value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to flip running flag for test", e);
        }
    }

    @Test
    void applyAdmittedEvent_recordsAdmitToFixNosTimer_andAdvancesCursor() throws Exception {
        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY);
        NewOrderSingle nos = new NewOrderSingle();
        when(builder.build(ev)).thenReturn(nos);
        when(send.hasActiveSession()).thenReturn(true);

        boolean advanced = service.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        verify(send).send(nos);
        verify(cursorRepository)
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, FRAGMENT_POSITION);

        Timer timer = meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_FIX_NOS)
                .tags(Tags.of(
                        OmsPipelineMetrics.TAG_EGRESS_ID, OmsFixEgressService.EGRESS_ID,
                        OmsPipelineMetrics.TAG_SIDE, "buy",
                        OmsPipelineMetrics.TAG_TIF, "day"))
                .timer();
        assertThat(timer)
                .as("admit-to-fix-nos Timer registered with expected (egress, side, tif) tags")
                .isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .isEqualTo((double) (NOW_MS - ACCEPTED_AT_MS));
    }

    @Test
    void applyAdmittedEvent_negativeLatencyClampedToZero() throws Exception {
        // Wall-clock now < admit timestamp simulates rare NTP slew backwards. Timer must still
        // record a non-negative duration; otherwise Micrometer rejects the sample.
        long acceptedInTheFuture = NOW_MS + 5_000L;
        OrderAdmittedEvent ev = new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ acceptedInTheFuture,
                /* quantityScaled = */ 1L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                /* version = */ 0,
                AcceptOrderCommand.SIDE_SELL,
                AcceptOrderCommand.TIF_IOC,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
        when(builder.build(ev)).thenReturn(new NewOrderSingle());
        when(send.hasActiveSession()).thenReturn(true);

        boolean advanced = service.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        Timer timer = meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_FIX_NOS)
                .tags(Tags.of(
                        OmsPipelineMetrics.TAG_EGRESS_ID, OmsFixEgressService.EGRESS_ID,
                        OmsPipelineMetrics.TAG_SIDE, "sell",
                        OmsPipelineMetrics.TAG_TIF, "ioc"))
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .as("Math.max(0, ...) clamp keeps NTP-slew samples at 0 ms instead of negative")
                .isEqualTo(0.0);
    }

    @Test
    void applyAdmittedEvent_cursorOnlyMode_doesNotRecordFixNosTimer() {
        // Drop the FIX beans (context-only IT shape) — service falls back to cursor-only behaviour
        // and the per-event Timer must stay silent because no NOS was actually emitted.
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
        OmsFixEgressService cursorOnly = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                pinned,
                /* builder = */ null,
                /* orderCancelRequestBuilder = */ null,
                /* orderCancelReplaceRequestBuilder = */ null,
                /* send = */ null);
        flipRunning(cursorOnly, true);

        OrderAdmittedEvent ev = sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY);
        boolean advanced = cursorOnly.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        verify(cursorRepository)
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, FRAGMENT_POSITION);
        assertThat(meterRegistry.find(OmsPipelineMeterNames.CLUSTER_ADMIT_TO_FIX_NOS).timer())
                .as("cursor-only mode emits no NOS — admit-to-fix-nos Timer must stay unregistered")
                .isNull();
    }

    @Test
    void applyAdmittedEvent_batchedCursorFlushEvery3_persistsOnceEvery3rdFragment() throws Exception {
        // Slice 4l H2: with cursorFlushEvery=3 the Postgres UPSERT to oms_fix_egress_cursor must
        // run on fragments 3, 6, 9 (and any final flush on shutdown), not on fragments 1/2/4/5/7/8.
        // We carry the latest fragment position across the gap so the durable cursor never
        // points behind a fragment that has already shipped to the broker.
        OmsFixEgressService batched = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC),
                builder,
                /* orderCancelRequestBuilder = */ null,
                /* orderCancelReplaceRequestBuilder = */ null,
                send);
        setCursorFlushEvery(batched, 3);
        flipRunning(batched, true);

        when(builder.build(any(OrderAdmittedEvent.class))).thenReturn(new NewOrderSingle());
        when(send.hasActiveSession()).thenReturn(true);

        long pos1 = 1024L;
        long pos2 = 2048L;
        long pos3 = 3072L;
        long pos4 = 4096L;
        long pos5 = 5120L;
        long pos6 = 6144L;

        for (long pos : new long[] {pos1, pos2}) {
            assertThat(batched.applyAdmittedEvent(sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY), pos)).isTrue();
        }
        verifyNoInteractions(cursorRepository);

        assertThat(batched.applyAdmittedEvent(sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY), pos3)).isTrue();
        verify(cursorRepository, times(1))
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos3);

        for (long pos : new long[] {pos4, pos5}) {
            assertThat(batched.applyAdmittedEvent(sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY), pos)).isTrue();
        }
        verify(cursorRepository, never())
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos4);
        verify(cursorRepository, never())
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos5);

        assertThat(batched.applyAdmittedEvent(sampleAdmitted(AcceptOrderCommand.SIDE_BUY, AcceptOrderCommand.TIF_DAY), pos6)).isTrue();
        verify(cursorRepository, times(1))
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos6);

        verifyNoMoreInteractions(cursorRepository);
    }

    private static void setCursorFlushEvery(OmsFixEgressService svc, int value) {
        try {
            Field f = OmsFixEgressService.class.getDeclaredField("cursorFlushEvery");
            f.setAccessible(true);
            f.setInt(svc, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to set cursorFlushEvery for test", e);
        }
    }

    private static OrderAdmittedEvent sampleAdmitted(byte side, byte tif) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                /* quantityScaled = */ 1L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                /* version = */ 0,
                side,
                tif,
                "acct",
                "idem",
                "hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }

    // ========================================================================
    // Wed-demo: cancel + replace request event dispatch
    // ========================================================================

    @Test
    void applyCancelRequestedEvent_sendsOrderCancelRequest_andAdvancesCursor() throws Exception {
        FixOrderCancelRequestBuilder cancelBuilder = org.mockito.Mockito.mock(FixOrderCancelRequestBuilder.class);
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
        OmsFixEgressService egress = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                pinned,
                /* nosBuilder = */ null,
                cancelBuilder,
                /* replaceBuilder = */ null,
                send);
        flipRunning(egress, true);

        OrderCancelRequestedEvent ev = sampleCancelRequested();
        OrderCancelRequest msg = new OrderCancelRequest();
        when(cancelBuilder.build(ev)).thenReturn(msg);
        when(send.hasActiveSession()).thenReturn(true);

        boolean advanced = egress.applyCancelRequestedEvent(ev, FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        verify(send).send(msg);
        verify(cursorRepository)
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, FRAGMENT_POSITION);
    }

    @Test
    void applyReplaceRequestedEvent_sendsOrderCancelReplaceRequest_andAdvancesCursor() throws Exception {
        FixOrderCancelReplaceRequestBuilder replaceBuilder =
                org.mockito.Mockito.mock(FixOrderCancelReplaceRequestBuilder.class);
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
        OmsFixEgressService egress = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                pinned,
                /* nosBuilder = */ null,
                /* cancelBuilder = */ null,
                replaceBuilder,
                send);
        flipRunning(egress, true);

        OrderReplaceRequestedEvent ev = sampleReplaceRequested();
        OrderCancelReplaceRequest msg = new OrderCancelReplaceRequest();
        when(replaceBuilder.build(ev)).thenReturn(msg);
        when(send.hasActiveSession()).thenReturn(true);

        boolean advanced = egress.applyReplaceRequestedEvent(ev, FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        verify(send).send(msg);
        verify(cursorRepository)
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, FRAGMENT_POSITION);
    }

    @Test
    void applyCancelRequestedEvent_buildersAbsent_cursorOnlyFallback() {
        // Mirror applyAdmittedEvent_cursorOnlyMode: the cancel-request dispatch must also tolerate
        // missing FIX beans (context-only ITs with oms.routing.backend=noop) and still advance the
        // cursor so the replay loop does not stall.
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC);
        OmsFixEgressService cursorOnly = new OmsFixEgressService(
                config,
                cursorRepository,
                meterRegistry,
                pinned,
                /* nosBuilder = */ null,
                /* cancelBuilder = */ null,
                /* replaceBuilder = */ null,
                /* send = */ null);
        flipRunning(cursorOnly, true);

        boolean advanced = cursorOnly.applyCancelRequestedEvent(sampleCancelRequested(), FRAGMENT_POSITION);

        assertThat(advanced).isTrue();
        verify(cursorRepository)
                .advance(OmsFixEgressService.EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, FRAGMENT_POSITION);
        verifyNoInteractions(send);
    }

    private static OrderCancelRequestedEvent sampleCancelRequested() {
        return new OrderCancelRequestedEvent(
                UUID.randomUUID(),
                /* originalQuantityScaled = */ 1_000_000_000L,
                /* cumQtyScaled = */ 0L,
                /* requestedAtMillis = */ ACCEPTED_AT_MS,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                "acct",
                "AAPL",
                "idem-cancel",
                "user-cancel");
    }

    private static OrderReplaceRequestedEvent sampleReplaceRequested() {
        return new OrderReplaceRequestedEvent(
                UUID.randomUUID(),
                /* originalQuantityScaled = */ 1_000_000_000L,
                /* originalLimitPriceScaledOrZero = */ 100_000_000L,
                /* newQuantityScaled = */ 1_500_000_000L,
                /* newLimitPriceScaledOrZero = */ 120_000_000L,
                /* requestedAtMillis = */ ACCEPTED_AT_MS,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "acct",
                "AAPL",
                "idem-replace",
                "operator-resize");
    }
}
