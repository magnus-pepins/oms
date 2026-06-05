package com.balh.oms.venueegress;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.venue.VenueRouteOrderClient;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Evidence for the route-offer half of {@link OmsVenueEgressService.EgressRoutePipeline}:
 * ordered stream writes cap at one consumer thread; tracker registration belongs on the replay thread.
 */
@ExtendWith(MockitoExtension.class)
class OmsVenueEgressRouteOfferCapTest {

    private static final long SIMULATED_OFFER_LATENCY_NANOS = 2_500_000L;

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
    }

    @Test
    void trackerRegister_onReplayThread_beforeStreamOfferRuns() throws Exception {
        CountDownLatch offerEntered = new CountDownLatch(1);
        CountDownLatch releaseOffer = new CountDownLatch(1);
        var routeOfferPool = Executors.newSingleThreadExecutor();
        try {
            service.enablePipelineForTesting(4, Runnable::run, Runnable::run, routeOfferPool);

            when(routeClient.routeAdmittedOrderAsync(any()))
                    .thenAnswer(
                            inv -> {
                                offerEntered.countDown();
                                assertThat(releaseOffer.await(5, TimeUnit.SECONDS)).isTrue();
                                OrderAdmittedEvent ev = inv.getArgument(0);
                                return CompletableFuture.completedFuture(Optional.of(er(ev)));
                            });

            service.pipelineDispatchAdmitForTesting(admit("PREDMKT-TEST-1"), 10L);

            // Registration moved to replay thread: in-flight count rises before the offer consumer runs.
            assertThat(service.pipelineInFlightForTesting()).isEqualTo(1);
            assertThat(offerEntered.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseOffer.countDown();
            routeOfferPool.shutdown();
            routeOfferPool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void streamOffers_remainStrictlyOrdered_onSingleOfferConsumer() {
        int admitCount = 8;
        List<OrderAdmittedEvent> events = new ArrayList<>();
        for (int i = 0; i < admitCount; i++) {
            OrderAdmittedEvent ev = admit("PREDMKT-TEST-1");
            events.add(ev);
            when(routeClient.routeAdmittedOrderAsync(ev))
                    .thenReturn(CompletableFuture.completedFuture(Optional.of(er(ev))));
        }

        var routeOfferPool = Executors.newSingleThreadExecutor();
        try {
            service.enablePipelineForTesting(admitCount, Runnable::run, Runnable::run, routeOfferPool);
            service.markRunningForTesting();

            for (int i = 0; i < admitCount; i++) {
                service.pipelineDispatchAdmitForTesting(events.get(i), 10L * (i + 1));
            }
            assertThat(service.pipelineQuiesceForTesting()).isTrue();

            InOrder order = inOrder(routeClient);
            for (OrderAdmittedEvent ev : events) {
                order.verify(routeClient).routeAdmittedOrderAsync(ev);
            }
        } finally {
            routeOfferPool.shutdown();
        }
    }

    @Test
    void singleOfferConsumer_throughputBoundedByPerOfferLatency() throws Exception {
        int admitCount = 20;
        CountDownLatch offersDone = new CountDownLatch(admitCount);
        List<OrderAdmittedEvent> events = new ArrayList<>();
        for (int i = 0; i < admitCount; i++) {
            OrderAdmittedEvent ev = admit("PREDMKT-TEST-1");
            events.add(ev);
            when(routeClient.routeAdmittedOrderAsync(ev))
                    .thenAnswer(
                            inv -> {
                                LockSupport.parkNanos(SIMULATED_OFFER_LATENCY_NANOS);
                                offersDone.countDown();
                                return CompletableFuture.completedFuture(Optional.of(er(ev)));
                            });
        }

        var routeOfferPool = Executors.newSingleThreadExecutor();
        try {
            service.enablePipelineForTesting(admitCount, Runnable::run, Runnable::run, routeOfferPool);
            service.markRunningForTesting();

            long t0 = System.nanoTime();
            for (int i = 0; i < admitCount; i++) {
                service.pipelineDispatchAdmitForTesting(events.get(i), 10L * (i + 1));
            }
            assertThat(offersDone.await(30, TimeUnit.SECONDS)).isTrue();
            long offerSpanNanos = System.nanoTime() - t0;

            long serialFloorNanos = SIMULATED_OFFER_LATENCY_NANOS * admitCount;
            assertThat(offerSpanNanos)
                    .as("single writer: offer span >= N * per-offer latency")
                    .isGreaterThanOrEqualTo(serialFloorNanos);
            double observedOfferRps = admitCount / (offerSpanNanos / 1_000_000_000.0);
            double serialCeilingRps = 1_000_000_000.0 / SIMULATED_OFFER_LATENCY_NANOS;
            assertThat(observedOfferRps)
                    .as("observed offer RPS tracks 1/latency serial ceiling (~400/s at 2.5ms)")
                    .isLessThan(serialCeilingRps * 1.2);
            assertThat(observedOfferRps)
                    .as("parallel stream writes would exceed serial ceiling materially")
                    .isLessThan(serialCeilingRps * 2.0);
        } finally {
            routeOfferPool.shutdown();
            routeOfferPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void replayThreadCanEnqueueFasterThanOfferConsumer_drainsSerially() throws Exception {
        int maxInFlight = 8;
        CountDownLatch releaseOffers = new CountDownLatch(1);
        AtomicInteger concurrentOffers = new AtomicInteger();
        AtomicInteger maxConcurrentOffers = new AtomicInteger();

        when(routeClient.routeAdmittedOrderAsync(any()))
                .thenAnswer(
                        inv -> {
                            int concurrent = concurrentOffers.incrementAndGet();
                            maxConcurrentOffers.updateAndGet(prev -> Math.max(prev, concurrent));
                            try {
                                releaseOffers.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return CompletableFuture.failedFuture(e);
                            } finally {
                                concurrentOffers.decrementAndGet();
                            }
                            OrderAdmittedEvent ev = inv.getArgument(0);
                            return CompletableFuture.completedFuture(Optional.of(er(ev)));
                        });

        var routeOfferPool = Executors.newSingleThreadExecutor();
        try {
            service.enablePipelineForTesting(maxInFlight, Runnable::run, Runnable::run, routeOfferPool);
            service.markRunningForTesting();

            for (int i = 0; i < maxInFlight; i++) {
                service.pipelineDispatchAdmitForTesting(admit("PREDMKT-TEST-1"), 10L * (i + 1));
            }

            Thread.sleep(100L);
            assertThat(maxConcurrentOffers.get())
                    .as("RouteOrderStream writer is single-threaded")
                    .isEqualTo(1);

            releaseOffers.countDown();
            assertThat(service.pipelineQuiesceForTesting()).isTrue();
            verify(routeClient, times(maxInFlight)).routeAdmittedOrderAsync(any());
        } finally {
            releaseOffers.countDown();
            routeOfferPool.shutdown();
            routeOfferPool.awaitTermination(2, TimeUnit.SECONDS);
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
