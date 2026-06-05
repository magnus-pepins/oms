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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
    void cursorOnlyBetweenAdmits_doesNotBlockLaterDispatch() throws Exception {
        OrderAdmittedEvent ev1 = admit("PREDMKT-TEST-1");
        OrderAdmittedEvent ev2 = admit("PREDMKT-TEST-1");
        CompletableFuture<Optional<ExecutionReport>> f1 = new CompletableFuture<>();
        CompletableFuture<Optional<ExecutionReport>> f2 = new CompletableFuture<>();
        when(routeClient.routeAdmittedOrderAsync(ev1)).thenReturn(f1);
        when(routeClient.routeAdmittedOrderAsync(ev2)).thenReturn(f2);

        service.pipelineDispatchAdmitForTesting(ev1, 10L);
        service.pipelineRegisterCursorOnlyForTesting(15L);
        service.pipelineDispatchAdmitForTesting(ev2, 20L);

        verify(routeClient, times(2)).routeAdmittedOrderAsync(any());
        assertThat(service.pipelineIsDrainedForTesting()).isFalse();

        f1.complete(Optional.of(er(ev1)));
        f2.complete(Optional.of(er(ev2)));
        service.pipelineDrainContiguousForTesting();

        verify(cursorRepository, times(1)).advanceWithRecording(any(), anyInt(), eq(3L), eq(20L));
        assertThat(service.pipelineIsDrainedForTesting()).isTrue();
    }

    @Test
    void manyCompletions_coalesceScheduledCursorDrainToSingleJdbcFlush() throws Exception {
        int admitCount = 32;
        List<OrderAdmittedEvent> events = new ArrayList<>();
        List<CompletableFuture<Optional<ExecutionReport>>> futures = new ArrayList<>();
        for (int i = 0; i < admitCount; i++) {
            OrderAdmittedEvent ev = admit("PREDMKT-TEST-1");
            events.add(ev);
            CompletableFuture<Optional<ExecutionReport>> f = new CompletableFuture<>();
            futures.add(f);
            when(routeClient.routeAdmittedOrderAsync(ev)).thenReturn(f);
        }

        AtomicInteger jdbcCalls = new AtomicInteger();
        when(cursorRepository.advanceWithRecording(any(), anyInt(), eq(3L), anyLong()))
                .thenAnswer(
                        inv -> {
                            jdbcCalls.incrementAndGet();
                            return true;
                        });

        AtomicInteger cursorDrainSubmissions = new AtomicInteger();
        CountDownLatch releaseCursorDrain = new CountDownLatch(1);
        var cursorDrainPool = Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Executor cursorDrainExecutor =
                    task -> {
                        cursorDrainSubmissions.incrementAndGet();
                        cursorDrainPool.execute(
                                () -> {
                                    try {
                                        releaseCursorDrain.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                    task.run();
                                });
                    };
            service.enablePipelineForTesting(32, Runnable::run, cursorDrainExecutor);

            for (int i = 0; i < admitCount; i++) {
                service.pipelineDispatchAdmitForTesting(events.get(i), 10L * (i + 1));
            }

            for (int i = 0; i < admitCount; i++) {
                futures.get(i).complete(Optional.of(er(events.get(i))));
            }

            releaseCursorDrain.countDown();

            service.markRunningForTesting();
            assertThat(service.pipelineQuiesceForTesting()).isTrue();

            // Coalesced: one queued drain task (not one per completion) and one JDBC flush.
            assertThat(cursorDrainSubmissions.get()).isEqualTo(1);
            assertThat(jdbcCalls.get()).isEqualTo(1);
            verify(cursorRepository, times(1))
                    .advanceWithRecording(any(), anyInt(), eq(3L), eq(10L * admitCount));
        } finally {
            releaseCursorDrain.countDown();
            cursorDrainPool.shutdown();
            cursorDrainPool.awaitTermination(2, TimeUnit.SECONDS);
        }
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

    @Test
    void dispatchDecouplesReplayFromBlockingGrpcOffer() throws Exception {
        int maxInFlight = 4;
        CountDownLatch releaseOffer = new CountDownLatch(1);
        AtomicInteger offersInFlight = new AtomicInteger();
        AtomicInteger maxConcurrentOffers = new AtomicInteger();
        var routeOfferPool = Executors.newSingleThreadExecutor();
        var erPool = Executors.newSingleThreadExecutor();
        try {
            service.enablePipelineForTesting(4, erPool, Runnable::run, routeOfferPool);

            when(routeClient.routeAdmittedOrderAsync(any()))
                    .thenAnswer(
                            inv -> {
                                int concurrent = offersInFlight.incrementAndGet();
                                maxConcurrentOffers.updateAndGet(prev -> Math.max(prev, concurrent));
                                try {
                                    releaseOffer.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return CompletableFuture.failedFuture(e);
                                } finally {
                                    offersInFlight.decrementAndGet();
                                }
                                OrderAdmittedEvent ev = inv.getArgument(0);
                                return CompletableFuture.completedFuture(Optional.of(er(ev)));
                            });

            long t0 = System.nanoTime();
            for (int i = 0; i < maxInFlight; i++) {
                service.pipelineDispatchAdmitForTesting(admit("PREDMKT-TEST-1"), 10L * (i + 1));
            }
            long enqueueNanos = System.nanoTime() - t0;

            assertThat(TimeUnit.NANOSECONDS.toMillis(enqueueNanos)).isLessThan(100L);
            await().atMost(Duration.ofSeconds(1)).until(() -> maxConcurrentOffers.get() >= 1);

            releaseOffer.countDown();
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> verify(routeClient, times(maxInFlight)).routeAdmittedOrderAsync(any()));
            service.markRunningForTesting();
            assertThat(service.pipelineQuiesceForTesting()).isTrue();
        } finally {
            releaseOffer.countDown();
            routeOfferPool.shutdown();
            erPool.shutdown();
            routeOfferPool.awaitTermination(2, TimeUnit.SECONDS);
            erPool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void slowClusterErOffer_dispatchesFullPendingWindowWhileOffersPark() throws Exception {
        int maxInFlight = 4;
        int admitCount = maxInFlight * 2;
        CountDownLatch releaseEr = new CountDownLatch(1);

        doAnswer(
                        inv -> {
                            assertThat(releaseEr.await(5, TimeUnit.SECONDS)).isTrue();
                            return null;
                        })
                .when(clusterIngressClient)
                .submitApplyExecutionReport(any(), any());

        ExecutorService erPool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            service.enablePipelineForTesting(maxInFlight, erPool, Runnable::run, Runnable::run);
            service.markRunningForTesting();

            List<OrderAdmittedEvent> events = new ArrayList<>();
            for (int i = 0; i < admitCount; i++) {
                OrderAdmittedEvent ev = admit("PREDMKT-TEST-1");
                events.add(ev);
                when(routeClient.routeAdmittedOrderAsync(ev))
                        .thenReturn(CompletableFuture.completedFuture(Optional.of(er(ev))));
            }

            for (int i = 0; i < admitCount; i++) {
                service.pipelineDispatchAdmitForTesting(events.get(i), 10L * (i + 1));
            }

            assertThat(service.pipelineInFlightForTesting()).isEqualTo(admitCount);

            releaseEr.countDown();
            assertThat(service.pipelineQuiesceForTesting()).isTrue();
            verify(cursorRepository, times(1))
                    .advanceWithRecording(any(), anyInt(), eq(3L), eq(10L * admitCount));
        } finally {
            erPool.shutdown();
            erPool.awaitTermination(2, TimeUnit.SECONDS);
        }
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
