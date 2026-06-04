package com.balh.oms.venueegress;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.venue.VenueRouteOrderClient;
import com.balh.oms.venue.VenueRouteTransportException;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pipelined egress (Design A) behaviour with a caller-runs completion executor so async route acks
 * complete deterministically on the test thread.
 *
 * <p>Crux invariant under test: the persisted cursor advances <em>only</em> over the
 * contiguous-completed prefix. Out-of-order acks must never advance the cursor past a still-in-flight
 * fragment — a crash there must replay it. See
 * {@code system-documentation/plans/oms-venue-egress-pipelining.md}.
 */
@ExtendWith(MockitoExtension.class)
class OmsVenueEgressPipelineTest {

    @Mock OmsVenueEgressCursorRepository cursorRepository;
    @Mock VenueRouteOrderClient routeClient;
    @Mock OmsClusterIngressClient clusterIngressClient;

    OmsVenueEgressService service;

    @BeforeEach
    void setUp() {
        service =
                new OmsVenueEgressService(
                        new OmsConfig(),
                        cursorRepository,
                        new SimpleMeterRegistry(),
                        new ObjectMapper(),
                        Clock.systemUTC(),
                        routeClient,
                        clusterIngressClient);
        service.setCurrentRecordingIdForTesting(3L);
        lenient()
                .when(cursorRepository.advanceWithRecording(any(), anyInt(), eq(3L), anyLong()))
                .thenReturn(true);
        // Caller-runs executor: completing a future on the test thread runs the ack callback inline.
        service.enablePipelineForTesting(8, Runnable::run);
    }

    @Test
    void outOfOrderAcks_advanceCursorOnlyOverContiguousPrefix() throws Exception {
        OrderAdmittedEvent ev1 = admit("PREDMKT-TEST-1");
        OrderAdmittedEvent ev2 = admit("PREDMKT-TEST-1");
        OrderAdmittedEvent ev3 = admit("PREDMKT-TEST-1");
        CompletableFuture<Optional<ExecutionReport>> f1 = new CompletableFuture<>();
        CompletableFuture<Optional<ExecutionReport>> f2 = new CompletableFuture<>();
        CompletableFuture<Optional<ExecutionReport>> f3 = new CompletableFuture<>();
        when(routeClient.routeAdmittedOrderAsync(ev1)).thenReturn(f1);
        when(routeClient.routeAdmittedOrderAsync(ev2)).thenReturn(f2);
        when(routeClient.routeAdmittedOrderAsync(ev3)).thenReturn(f3);

        service.pipelineDispatchAdmitForTesting(ev1, 10L);
        service.pipelineDispatchAdmitForTesting(ev2, 20L);
        service.pipelineDispatchAdmitForTesting(ev3, 30L);

        // Routes are written to the venue stream in dispatch (cluster-log) order.
        InOrder order = inOrder(routeClient);
        order.verify(routeClient).routeAdmittedOrderAsync(ev1);
        order.verify(routeClient).routeAdmittedOrderAsync(ev2);
        order.verify(routeClient).routeAdmittedOrderAsync(ev3);

        // Acks 20 and 30 arrive before 10 — cursor MUST NOT advance (a crash here replays 10..30).
        f2.complete(Optional.of(er(ev2)));
        f3.complete(Optional.of(er(ev3)));
        service.pipelineDrainContiguousForTesting();
        verify(cursorRepository, never()).advanceWithRecording(any(), anyInt(), anyLong(), anyLong());
        assertThat(service.pipelineIsDrainedForTesting()).isFalse();

        // 10 completes — the whole contiguous run flushes once, to the highest position (30).
        f1.complete(Optional.of(er(ev1)));
        service.pipelineDrainContiguousForTesting();
        verify(cursorRepository, times(1)).advanceWithRecording(any(), anyInt(), eq(3L), eq(30L));
        verify(cursorRepository, never()).advanceWithRecording(any(), anyInt(), anyLong(), eq(10L));
        verify(cursorRepository, never()).advanceWithRecording(any(), anyInt(), anyLong(), eq(20L));
        assertThat(service.pipelineIsDrainedForTesting()).isTrue();

        // All three execution reports were submitted to the OMS cluster.
        verify(clusterIngressClient, times(3)).submitApplyExecutionReport(any(), any());
    }

    @Test
    void venueRejectInFlight_submitsVenueRejectAndStillAdvancesContiguously() throws Exception {
        OrderAdmittedEvent ev1 = admit("PREDMKT-TEST-1");
        OrderAdmittedEvent ev2 = admit("PREDMKT-TEST-1");
        CompletableFuture<Optional<ExecutionReport>> f1 = new CompletableFuture<>();
        CompletableFuture<Optional<ExecutionReport>> f2 = new CompletableFuture<>();
        when(routeClient.routeAdmittedOrderAsync(ev1)).thenReturn(f1);
        when(routeClient.routeAdmittedOrderAsync(ev2)).thenReturn(f2);

        service.pipelineDispatchAdmitForTesting(ev1, 10L);
        service.pipelineDispatchAdmitForTesting(ev2, 20L);

        // ev1 transport-fails, ev2 rejected (empty) — both still complete the prefix; the cursor
        // advances and a VENUE_REJECT is submitted for each (mirrors the serial path).
        f1.completeExceptionally(new VenueRouteTransportException("down", new RuntimeException()));
        f2.complete(Optional.empty());
        service.pipelineDrainContiguousForTesting();

        verify(cursorRepository, times(1)).advanceWithRecording(any(), anyInt(), eq(3L), eq(20L));
        verify(clusterIngressClient, times(2)).submitApplyExecutionReport(any(), any());
        assertThat(service.pipelineIsDrainedForTesting()).isTrue();
    }

    @Test
    void quiesce_blocksUntilInFlightCompletes_thenAdvances() throws Exception {
        service.markRunningForTesting();
        OrderAdmittedEvent ev1 = admit("PREDMKT-TEST-1");
        CompletableFuture<Optional<ExecutionReport>> f1 = new CompletableFuture<>();
        when(routeClient.routeAdmittedOrderAsync(ev1)).thenReturn(f1);

        service.pipelineDispatchAdmitForTesting(ev1, 10L);

        Thread completer =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            f1.complete(Optional.of(er(ev1)));
                        });
        completer.start();

        assertThat(service.pipelineQuiesceForTesting()).isTrue();
        completer.join(2_000L);

        assertThat(service.pipelineIsDrainedForTesting()).isTrue();
        verify(cursorRepository, times(1)).advanceWithRecording(any(), anyInt(), eq(3L), eq(10L));
    }

    private static OrderAdmittedEvent admit(String symbol) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                1L,
                1L,
                10_000_000_000L,
                650_000L,
                0,
                0,
                (byte) 0,
                (byte) 0,
                (byte) 2,
                "acct",
                "intent",
                "hash",
                symbol,
                null,
                null);
    }

    private static ExecutionReport er(OrderAdmittedEvent ev) {
        return ExecutionReport.newBuilder()
                .setOmsOrderId(ev.orderId().toString())
                .setVenueExecRef("venue-exec-" + ev.orderId())
                .setLastQtyScaled(10_000_000_000L)
                .setLastPxScaled(650_000L)
                .setVenueTsNanos(1L)
                .setExecType(ExecType.EXEC_TYPE_TRADE)
                .build();
    }
}
