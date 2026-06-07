package com.balh.oms.venue;

import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.balh.venue.grpc.v1.RouteOrderRequest;
import com.balh.venue.grpc.v1.RouteOrderResponse;
import com.balh.venue.grpc.v1.VenueOrderServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for pipelined {@code RouteOrderStream} client behaviour: non-blocking enqueue,
 * FIFO ordered writes, in-flight dedup, and ack demux.
 */
class VenueRouteOrderClientTest {

    private Server server;
    private int port;
    private RecordingRouteOrderStreamService streamService;
    private VenueRouteOrderClient client;

    @BeforeEach
    void setUp() throws Exception {
        streamService = new RecordingRouteOrderStreamService(0L);
        server = ServerBuilder.forPort(0).addService(streamService).build().start();
        port = server.getPort();
        OmsConfig config = new OmsConfig();
        config.getVenue().setGrpcHost("127.0.0.1");
        config.getVenue().setGrpcPort(port);
        config.getVenue().setGrpcStreamAckTimeoutMs(5_000L);
        client = new VenueRouteOrderClient(config, new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
            client = null;
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void routeAdmittedOrderAsync_doesNotBlockOnSlowServerAccept() throws Exception {
        streamService.setOnNextDelayMs(150L);
        int count = 24;
        long enqueueStart = System.nanoTime();
        List<CompletableFuture<Optional<ExecutionReport>>> futures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            futures.add(client.routeAdmittedOrderAsync(admit(UUID.randomUUID(), "PREDMKT-TEST-" + i)));
        }
        long enqueueNanos = System.nanoTime() - enqueueStart;
        // All enqueues must finish quickly — not N × server onNext delay (would be >3s if blocking).
        assertThat(TimeUnit.NANOSECONDS.toMillis(enqueueNanos))
                .as("enqueue must not block on gRPC onNext backpressure")
                .isLessThan(500L);

        for (CompletableFuture<Optional<ExecutionReport>> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS)).isPresent();
        }
        assertThat(streamService.receivedOrderIds()).hasSize(count);
    }

    @Test
    void routeAdmittedOrderAsync_preservesFifoWriteOrder() throws Exception {
        List<String> expected = new ArrayList<>();
        List<CompletableFuture<Optional<ExecutionReport>>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            UUID id = UUID.randomUUID();
            expected.add(id.toString());
            futures.add(client.routeAdmittedOrderAsync(admit(id, "PREDMKT-FIFO-" + i)));
        }
        for (CompletableFuture<Optional<ExecutionReport>> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS)).isPresent();
        }
        assertThat(streamService.receivedOrderIds()).containsExactlyElementsOf(expected);
    }

    @Test
    void routeAdmittedOrderAsync_rejectsDuplicateInFlightOrderId() throws Exception {
        UUID orderId = UUID.randomUUID();
        OrderAdmittedEvent ev = admit(orderId, "PREDMKT-DEDUP");
        streamService.holdAcksUntil(1);

        CompletableFuture<Optional<ExecutionReport>> first = client.routeAdmittedOrderAsync(ev);
        CompletableFuture<Optional<ExecutionReport>> second = client.routeAdmittedOrderAsync(ev);

        assertThatThrownBy(() -> second.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(VenueRouteTransportException.class)
                .hasMessageContaining("duplicate in-flight");

        streamService.releaseHeldAcks();
        assertThat(first.get(5, TimeUnit.SECONDS)).isPresent();
    }

    @Test
    void routeAdmittedOrderAsync_manyConcurrentAcksComplete() throws Exception {
        int count = 32;
        streamService.setResponseDelayMs(5L);
        List<CompletableFuture<Optional<ExecutionReport>>> futures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            futures.add(client.routeAdmittedOrderAsync(admit(UUID.randomUUID(), "PREDMKT-BURST-" + i)));
        }
        long t0 = System.nanoTime();
        for (CompletableFuture<Optional<ExecutionReport>> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS)).isPresent();
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        // Parallel demux (4 threads) must beat serializing 32 delayed acks on one gRPC thread.
        assertThat(elapsedMs).isLessThan(2_000L);
        assertThat(streamService.receivedOrderIds()).hasSize(count);
    }

    @Test
    void routeAdmittedOrderAsync_demuxesAcksByOmsOrderId() throws Exception {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        streamService.setResponseDelayMs(30L);

        CompletableFuture<Optional<ExecutionReport>> f1 = client.routeAdmittedOrderAsync(admit(id1, "PREDMKT-A"));
        CompletableFuture<Optional<ExecutionReport>> f2 = client.routeAdmittedOrderAsync(admit(id2, "PREDMKT-B"));

        Optional<ExecutionReport> er1 = f1.get(10, TimeUnit.SECONDS);
        Optional<ExecutionReport> er2 = f2.get(10, TimeUnit.SECONDS);
        assertThat(er1).isPresent();
        assertThat(er2).isPresent();
        assertThat(er1.get().getOmsOrderId()).isEqualTo(id1.toString());
        assertThat(er2.get().getOmsOrderId()).isEqualTo(id2.toString());
    }

    @Test
    void routeAdmittedOrderAsync_timesOutStaleInFlightRoute() throws Exception {
        client.shutdown();
        client = null;

        OmsConfig shortTimeoutConfig = new OmsConfig();
        shortTimeoutConfig.getVenue().setGrpcHost("127.0.0.1");
        shortTimeoutConfig.getVenue().setGrpcPort(port);
        shortTimeoutConfig.getVenue().setGrpcStreamAckTimeoutMs(400L);
        VenueRouteOrderClient shortClient =
                new VenueRouteOrderClient(shortTimeoutConfig, new SimpleMeterRegistry());
        try {
            streamService.setResponseDelayMs(30_000L);
            UUID orderId = UUID.randomUUID();
            CompletableFuture<Optional<ExecutionReport>> future =
                    shortClient.routeAdmittedOrderAsync(admit(orderId, "PREDMKT-TIMEOUT"));

            await().pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(future).isCompletedExceptionally());
            assertThatThrownBy(future::get)
                    .hasCauseInstanceOf(VenueRouteTransportException.class)
                    .hasMessageContaining("ack timeout");

            // Late ack after timeout must not re-complete the future.
            Thread.sleep(200L);
            assertThat(future).isCompletedExceptionally();
        } finally {
            shortClient.shutdown();
        }
    }

    private static OrderAdmittedEvent admit(UUID orderId, String symbol) {
        return new OrderAdmittedEvent(
                orderId,
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

    /**
     * Minimal bidi {@code RouteOrderStream} stub: records receive order, optional delays, optional
     * ack hold for timeout / dedup tests.
     */
    static final class RecordingRouteOrderStreamService
            extends VenueOrderServiceGrpc.VenueOrderServiceImplBase {

        private final List<String> receivedOrderIds = new ArrayList<>();
        private final ExecutorService responsePool = Executors.newSingleThreadExecutor();
        private volatile long onNextDelayMs;
        private volatile long responseDelayMs;
        private final AtomicInteger heldAcks = new AtomicInteger(0);
        private final List<Runnable> heldAckReleases = new ArrayList<>();

        RecordingRouteOrderStreamService(long onNextDelayMs) {
            this.onNextDelayMs = onNextDelayMs;
        }

        void setOnNextDelayMs(long ms) {
            this.onNextDelayMs = ms;
        }

        void setResponseDelayMs(long ms) {
            this.responseDelayMs = ms;
        }

        List<String> receivedOrderIds() {
            synchronized (receivedOrderIds) {
                return List.copyOf(receivedOrderIds);
            }
        }

        void holdAcksUntil(int count) {
            heldAcks.set(count);
        }

        void releaseHeldAcks() {
            List<Runnable> pending;
            synchronized (heldAckReleases) {
                pending = List.copyOf(heldAckReleases);
                heldAckReleases.clear();
                heldAcks.set(0);
            }
            pending.forEach(Runnable::run);
        }

        @Override
        public StreamObserver<RouteOrderRequest> routeOrderStream(
                StreamObserver<RouteOrderResponse> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(RouteOrderRequest request) {
                    if (onNextDelayMs > 0) {
                        try {
                            Thread.sleep(onNextDelayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    synchronized (receivedOrderIds) {
                        receivedOrderIds.add(request.getOmsOrderId());
                    }
                    Runnable emit =
                            () -> {
                                if (responseDelayMs > 0) {
                                    try {
                                        Thread.sleep(responseDelayMs);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                }
                                synchronized (responseObserver) {
                                    responseObserver.onNext(acceptResponse(request));
                                }
                            };
                    if (heldAcks.get() > 0) {
                        synchronized (heldAckReleases) {
                            heldAckReleases.add(emit);
                        }
                    } else {
                        responsePool.execute(emit);
                    }
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {
                    synchronized (responseObserver) {
                        responseObserver.onCompleted();
                    }
                }
            };
        }

        private static RouteOrderResponse acceptResponse(RouteOrderRequest request) {
            return RouteOrderResponse.newBuilder()
                    .setOmsOrderId(request.getOmsOrderId())
                    .setAccepted(true)
                    .setExecutionReport(
                            ExecutionReport.newBuilder()
                                    .setOmsOrderId(request.getOmsOrderId())
                                    .setExecType(ExecType.EXEC_TYPE_NEW)
                                    .build())
                    .build();
        }
    }
}
