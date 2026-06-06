package com.balh.oms.venue;

import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.observability.metrics.OmsVenueGrpcMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.balh.venue.grpc.v1.OrderLiveness;
import com.balh.venue.grpc.v1.QueryOrderStatusRequest;
import com.balh.venue.grpc.v1.QueryOrderStatusResponse;
import com.balh.venue.grpc.v1.RouteCancelRequest;
import com.balh.venue.grpc.v1.RouteCancelResponse;
import com.balh.venue.grpc.v1.RouteOrderRequest;
import com.balh.venue.grpc.v1.RouteOrderResponse;
import com.balh.venue.grpc.v1.RouteReplaceRequest;
import com.balh.venue.grpc.v1.RouteReplaceResponse;
import com.balh.venue.grpc.v1.VenueOrderServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "internal-venue")
public class VenueRouteOrderClient {

    private static final Logger log = LoggerFactory.getLogger(VenueRouteOrderClient.class);

    /** Bounded queue between callers and the ordered stream writer (venue gateway back-pressure). */
    private static final int ROUTE_STREAM_WRITE_QUEUE_CAPACITY = 8192;
    /** Max wait when the write queue is full before failing the route (fail closed → VENUE_REJECT). */
    private static final long WRITE_QUEUE_OFFER_TIMEOUT_MS = 100L;
    /** How often the ack-timeout sweeper scans {@link #pendingRoutes}. */
    private static final long ACK_TIMEOUT_SWEEP_INTERVAL_MS = 50L;
    /** Writer parks briefly when the gRPC stream is not ready (flow control). */
    private static final long STREAM_NOT_READY_PARK_NANOS = MILLISECONDS.toNanos(1L);
    /**
     * Idle park when the write queue is empty. Must use {@link LockSupport#parkNanos(long)} — not
     * {@link BlockingQueue#poll(long, TimeUnit)} — so {@link #routeAdmittedOrderAsync} can wake the
     * writer via {@link LockSupport#unpark(Thread)} without a missed signal (observed @ 10k/s:
     * {@code poll(50ms)} ignored unpark and inflated {@code oms.pipeline.cluster_admit_to_fix_nos}).
     */
    private static final long WRITE_LOOP_IDLE_PARK_NANOS = MILLISECONDS.toNanos(1L);
    /** Max ordered {@code onNext} calls per ready window before re-checking flow control. */
    private static final int MAX_WRITES_PER_READY_WINDOW = 64;

    private final ManagedChannel channel;
    private final VenueOrderServiceGrpc.VenueOrderServiceBlockingStub blockingStub;
    private final VenueOrderServiceGrpc.VenueOrderServiceStub asyncStub;
    private final long grpcCallTimeoutMs;
    private final long grpcStreamAckTimeoutMs;
    private final MeterRegistry meterRegistry;

    // ---- Pipelined RouteOrderStream state (used only when venue-route-max-in-flight > 1) ----
    /** In-flight async routes keyed by oms order id; completed by the response demuxer. */
    private final ConcurrentHashMap<String, PendingRoute> pendingRoutes = new ConcurrentHashMap<>();
    /** Ordered writes to {@link #requestObserver}; drained by the dedicated writer thread. */
    private final BlockingQueue<QueuedWrite> writeQueue =
            new LinkedBlockingQueue<>(ROUTE_STREAM_WRITE_QUEUE_CAPACITY);
    /** Guards stream (re)open and {@link ClientCallStreamObserver#setOnReadyHandler} wiring. */
    private final ReentrantLock streamLock = new ReentrantLock();
    private volatile StreamObserver<RouteOrderRequest> requestObserver;
    private volatile boolean shuttingDown;
    private final Thread writeThread;
    private ScheduledExecutorService routeTimeoutScheduler;

    private record QueuedWrite(RouteOrderRequest request, String orderId) {}

    private record PendingRoute(CompletableFuture<Optional<ExecutionReport>> future, long deadlineNanos) {}

    public VenueRouteOrderClient(OmsConfig omsConfig, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.grpcCallTimeoutMs = omsConfig.getVenue().getGrpcCallTimeoutMs();
        this.grpcStreamAckTimeoutMs = omsConfig.getVenue().getGrpcStreamAckTimeoutMs();
        this.channel =
                ManagedChannelBuilder.forAddress(
                                omsConfig.getVenue().getGrpcHost(), omsConfig.getVenue().getGrpcPort())
                        .usePlaintext()
                        .build();
        this.blockingStub = VenueOrderServiceGrpc.newBlockingStub(channel);
        this.asyncStub = VenueOrderServiceGrpc.newStub(channel);
        this.writeThread =
                new Thread(this::writeLoop, "venue-route-stream-writer");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        routeTimeoutScheduler()
                .scheduleAtFixedRate(
                        this::sweepAckTimeouts,
                        ACK_TIMEOUT_SWEEP_INTERVAL_MS,
                        ACK_TIMEOUT_SWEEP_INTERVAL_MS,
                        MILLISECONDS);
    }

    private VenueOrderServiceGrpc.VenueOrderServiceBlockingStub stubWithDeadline() {
        return blockingStub.withDeadlineAfter(grpcCallTimeoutMs, MILLISECONDS);
    }

    public Optional<ExecutionReport> routeAdmittedOrder(OrderAdmittedEvent ev) {
        RouteOrderRequest request =
                RouteOrderRequest.newBuilder()
                        .setOmsOrderId(ev.orderId().toString())
                        .setInstrumentSymbol(ev.instrumentSymbol())
                        .setQuantityScaled(ev.quantityScaled())
                        .setLimitPriceScaled(ev.limitPriceScaledOrZero())
                        .setSide(ev.side())
                        .setCounterpartyId(ev.accountId())
                        .build();
        RouteOrderResponse response;
        try {
            response = stubWithDeadline().routeOrder(request);
        } catch (StatusRuntimeException e) {
            OmsVenueGrpcMetrics.recordEgressFailure(meterRegistry, OmsVenueGrpcMetrics.RPC_ROUTE_ORDER, e);
            throw new VenueRouteTransportException(
                    "venue RouteOrder transport failure orderId=" + ev.orderId() + " symbol=" + ev.instrumentSymbol(),
                    e);
        }
        if (!response.getAccepted()) {
            log.warn(
                    "venue RouteOrder rejected orderId={} reason={}",
                    ev.orderId(),
                    response.getRejectReason());
            return Optional.empty();
        }
        if (!response.hasExecutionReport()) {
            return Optional.empty();
        }
        return Optional.of(response.getExecutionReport());
    }

    /**
     * Pipelined route over the ordered {@code RouteOrderStream}: enqueues the request for the
     * dedicated writer thread (preserving caller invocation order) and returns a future the
     * response demuxer completes when the venue acks. {@link Optional#empty()} means the venue
     * rejected / did not (fully) accept; a transport/stream failure completes the future
     * exceptionally with {@link VenueRouteTransportException} so the egress submits a VENUE_REJECT,
     * exactly as the blocking path does. Not thread-safe across callers — the single egress
     * route-offer thread is the only writer.
     */
    public CompletableFuture<Optional<ExecutionReport>> routeAdmittedOrderAsync(OrderAdmittedEvent ev) {
        String orderId = ev.orderId().toString();
        CompletableFuture<Optional<ExecutionReport>> future = new CompletableFuture<>();
        RouteOrderRequest request =
                RouteOrderRequest.newBuilder()
                        .setOmsOrderId(orderId)
                        .setInstrumentSymbol(ev.instrumentSymbol())
                        .setQuantityScaled(ev.quantityScaled())
                        .setLimitPriceScaled(ev.limitPriceScaledOrZero())
                        .setSide(ev.side())
                        .setCounterpartyId(ev.accountId())
                        .build();
        long deadlineNanos = System.nanoTime() + MILLISECONDS.toNanos(grpcStreamAckTimeoutMs);
        PendingRoute prev = pendingRoutes.putIfAbsent(orderId, new PendingRoute(future, deadlineNanos));
        if (prev != null) {
            future.completeExceptionally(
                    new VenueRouteTransportException(
                            "duplicate in-flight venue route orderId=" + orderId, new IllegalStateException()));
            return future;
        }
        try {
            if (!writeQueue.offer(new QueuedWrite(request, orderId), WRITE_QUEUE_OFFER_TIMEOUT_MS, MILLISECONDS)) {
                pendingRoutes.remove(orderId);
                future.completeExceptionally(
                        new VenueRouteTransportException(
                                "venue RouteOrderStream write queue full orderId=" + orderId,
                                new IllegalStateException("write queue full")));
                return future;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingRoutes.remove(orderId);
            future.completeExceptionally(
                    new VenueRouteTransportException(
                            "venue RouteOrderStream enqueue interrupted orderId=" + orderId, e));
            return future;
        }
        LockSupport.unpark(writeThread);
        return future;
    }

    /**
     * Drains {@link #writeQueue} in FIFO order, respecting gRPC client flow control so
     * {@link #routeAdmittedOrderAsync} never blocks on {@code onNext} backpressure.
     */
    private void writeLoop() {
        while (!shuttingDown) {
            QueuedWrite item = writeQueue.poll();
            if (item == null) {
                LockSupport.parkNanos(WRITE_LOOP_IDLE_PARK_NANOS);
                continue;
            }
            writeOne(item);
        }
    }

    private void writeOne(QueuedWrite item) {
        streamLock.lock();
        try {
            if (shuttingDown) {
                failQueuedWriteLocked(item, new IllegalStateException("venue route client shutting down"));
                return;
            }
            try {
                ensureStreamOpenLocked();
            } catch (RuntimeException e) {
                failQueuedWriteLocked(item, e);
                return;
            }
            ClientCallStreamObserver<RouteOrderRequest> callObserver =
                    (ClientCallStreamObserver<RouteOrderRequest>) requestObserver;
            while (!callObserver.isReady() && !shuttingDown) {
                streamLock.unlock();
                LockSupport.parkNanos(STREAM_NOT_READY_PARK_NANOS);
                streamLock.lock();
                if (requestObserver == null) {
                    failQueuedWriteLocked(item, new IllegalStateException("stream torn down"));
                    return;
                }
                callObserver = (ClientCallStreamObserver<RouteOrderRequest>) requestObserver;
            }
            if (shuttingDown) {
                failQueuedWriteLocked(item, new IllegalStateException("venue route client shutting down"));
                return;
            }
            try {
                callObserver.onNext(item.request());
                drainReadyWritesLocked(callObserver);
            } catch (RuntimeException e) {
                failQueuedWriteLocked(item, e);
            }
        } finally {
            streamLock.unlock();
        }
    }

    /**
     * Caller holds {@link #streamLock} and has just written {@code item}. While outbound flow
     * control allows, drain additional queued writes without releasing the lock — amortises
     * {@code isReady} spins and lock trips at 10k admits/s.
     */
    private void drainReadyWritesLocked(ClientCallStreamObserver<RouteOrderRequest> callObserver) {
        int written = 0;
        while (written < MAX_WRITES_PER_READY_WINDOW && callObserver.isReady() && !shuttingDown) {
            QueuedWrite next = writeQueue.poll();
            if (next == null) {
                return;
            }
            try {
                callObserver.onNext(next.request());
                written++;
            } catch (RuntimeException e) {
                failQueuedWriteLocked(next, e);
                return;
            }
        }
    }

    private void failQueuedWriteLocked(QueuedWrite item, RuntimeException e) {
        PendingRoute pr = pendingRoutes.remove(item.orderId());
        if (pr != null) {
            OmsVenueGrpcMetrics.recordEgressFailure(
                    meterRegistry, OmsVenueGrpcMetrics.RPC_ROUTE_ORDER, asStatusRuntime(e));
            pr.future()
                    .completeExceptionally(
                            new VenueRouteTransportException(
                                    "venue RouteOrderStream write failed orderId=" + item.orderId(), e));
        }
    }

    private ScheduledExecutorService routeTimeoutScheduler() {
        if (routeTimeoutScheduler == null) {
            routeTimeoutScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "venue-route-stream-timeout");
                                t.setDaemon(true);
                                return t;
                            });
        }
        return routeTimeoutScheduler;
    }

    /** One periodic sweep replaces per-order {@code schedule()} at 400+ routes/s. */
    private void sweepAckTimeouts() {
        long now = System.nanoTime();
        for (var entry : pendingRoutes.entrySet()) {
            if (entry.getValue().deadlineNanos() > now) {
                continue;
            }
            String orderId = entry.getKey();
            PendingRoute pr = pendingRoutes.remove(orderId);
            if (pr != null) {
                pr.future()
                        .completeExceptionally(
                                new VenueRouteTransportException(
                                        "venue RouteOrderStream ack timeout orderId=" + orderId,
                                        new java.util.concurrent.TimeoutException("ack timeout")));
            }
        }
    }

    /** Caller holds {@link #streamLock}. Opens (or reopens) the bidi stream if not currently live. */
    private void ensureStreamOpenLocked() {
        if (requestObserver != null) {
            return;
        }
        if (shuttingDown) {
            throw new IllegalStateException("venue route client shutting down");
        }
        requestObserver = asyncStub.routeOrderStream(new RouteStreamResponseObserver());
    }

    /** gRPC callback: unpark the writer when outbound flow-control window opens. */
    private void onStreamReady() {
        LockSupport.unpark(writeThread);
    }

    /** Demuxes async venue responses to the matching in-flight future by oms order id. */
    private final class RouteStreamResponseObserver
            implements ClientResponseObserver<RouteOrderRequest, RouteOrderResponse> {
        @Override
        public void beforeStart(ClientCallStreamObserver<RouteOrderRequest> requestStream) {
            requestStream.setOnReadyHandler(VenueRouteOrderClient.this::onStreamReady);
        }

        @Override
        public void onNext(RouteOrderResponse response) {
            PendingRoute pr = pendingRoutes.remove(response.getOmsOrderId());
            if (pr == null) {
                return; // late/duplicate (e.g. after a timeout already fired)
            }
            if (!response.getAccepted() || !response.hasExecutionReport()) {
                pr.future().complete(Optional.empty());
            } else {
                pr.future().complete(Optional.of(response.getExecutionReport()));
            }
        }

        @Override
        public void onError(Throwable t) {
            failAllPending("venue RouteOrderStream error", t);
        }

        @Override
        public void onCompleted() {
            failAllPending(
                    "venue RouteOrderStream closed by server",
                    new IllegalStateException("server completed stream"));
        }
    }

    /**
     * Tears down the broken stream and fails every in-flight route so the egress submits VENUE_REJECT
     * for each (at-least-once-at-venue: those orders are retried from the cursor on the next pass).
     * The next {@link #routeAdmittedOrderAsync} reopens a fresh stream.
     */
    private void failAllPending(String message, Throwable cause) {
        streamLock.lock();
        try {
            requestObserver = null;
        } finally {
            streamLock.unlock();
        }
        QueuedWrite queued;
        while ((queued = writeQueue.poll()) != null) {
            PendingRoute pr = pendingRoutes.remove(queued.orderId());
            if (pr != null) {
                pr.future()
                        .completeExceptionally(
                                new VenueRouteTransportException(
                                        message + " orderId=" + queued.orderId() + " (queued)", cause));
            }
        }
        List<String> ids = new ArrayList<>(pendingRoutes.keySet());
        for (String id : ids) {
            PendingRoute pr = pendingRoutes.remove(id);
            if (pr != null) {
                pr.future()
                        .completeExceptionally(
                                new VenueRouteTransportException(message + " orderId=" + id, cause));
            }
        }
        LockSupport.unpark(writeThread);
    }

    private static StatusRuntimeException asStatusRuntime(RuntimeException e) {
        if (e instanceof StatusRuntimeException sre) {
            return sre;
        }
        return io.grpc.Status.UNKNOWN.withDescription(e.getMessage()).withCause(e).asRuntimeException();
    }

    public Optional<ExecutionReport> routeCancelRequested(OrderCancelRequestedEvent ev) {
        RouteCancelResponse response;
        try {
            response =
                    stubWithDeadline().routeCancel(
                            RouteCancelRequest.newBuilder()
                                    .setOmsOrderId(ev.orderId().toString())
                                    .setInstrumentSymbol(ev.instrumentSymbol())
                                    .build());
        } catch (StatusRuntimeException e) {
            OmsVenueGrpcMetrics.recordEgressFailure(meterRegistry, OmsVenueGrpcMetrics.RPC_ROUTE_CANCEL, e);
            throw new VenueRouteTransportException(
                    "venue RouteCancel transport failure orderId=" + ev.orderId() + " symbol=" + ev.instrumentSymbol(),
                    e);
        }
        return acceptedExecutionReport(response.getAccepted(), response.getRejectReason(), ev.orderId(), response.hasExecutionReport(), response.getExecutionReport());
    }

    public Optional<ExecutionReport> routeReplaceRequested(OrderReplaceRequestedEvent ev) {
        RouteReplaceResponse response;
        try {
            response =
                    stubWithDeadline().routeReplace(
                            RouteReplaceRequest.newBuilder()
                                    .setOmsOrderId(ev.orderId().toString())
                                    .setInstrumentSymbol(ev.instrumentSymbol())
                                    .setNewQuantityScaled(ev.newQuantityScaled())
                                    .setNewLimitPriceScaled(ev.newLimitPriceScaledOrZero())
                                    .setSide(ev.sideCode())
                                    .build());
        } catch (StatusRuntimeException e) {
            OmsVenueGrpcMetrics.recordEgressFailure(meterRegistry, OmsVenueGrpcMetrics.RPC_ROUTE_REPLACE, e);
            throw new VenueRouteTransportException(
                    "venue RouteReplace transport failure orderId=" + ev.orderId() + " symbol=" + ev.instrumentSymbol(),
                    e);
        }
        return acceptedExecutionReport(response.getAccepted(), response.getRejectReason(), ev.orderId(), response.hasExecutionReport(), response.getExecutionReport());
    }

    /**
     * Golden-copy liveness check for one order: ask the venue whether {@code orderId} is still
     * resting for {@code instrumentSymbol}. Used by {@code VenueOrderReconciler}. Returns the venue's
     * {@link OrderLiveness}; transport failures propagate as {@link VenueRouteTransportException} so
     * the reconciler skips (never terminates an order on an inconclusive answer).
     */
    public OrderLiveness queryOrderStatus(UUID orderId, String instrumentSymbol) {
        QueryOrderStatusResponse response;
        try {
            response =
                    stubWithDeadline().queryOrderStatus(
                            QueryOrderStatusRequest.newBuilder()
                                    .setOmsOrderId(orderId.toString())
                                    .setInstrumentSymbol(instrumentSymbol)
                                    .build());
        } catch (StatusRuntimeException e) {
            OmsVenueGrpcMetrics.recordEgressFailure(meterRegistry, OmsVenueGrpcMetrics.RPC_QUERY_ORDER_STATUS, e);
            throw new VenueRouteTransportException(
                    "venue QueryOrderStatus transport failure orderId=" + orderId + " symbol=" + instrumentSymbol, e);
        }
        return response.getLiveness();
    }

    private Optional<ExecutionReport> acceptedExecutionReport(
            boolean accepted, String rejectReason, java.util.UUID orderId, boolean hasEr, ExecutionReport er) {
        if (!accepted) {
            log.warn("venue request rejected orderId={} reason={}", orderId, rejectReason);
            return Optional.empty();
        }
        if (!hasEr) {
            return Optional.empty();
        }
        return Optional.of(er);
    }

    @PreDestroy
    void shutdown() {
        shuttingDown = true;
        writeThread.interrupt();
        try {
            writeThread.join(2_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        streamLock.lock();
        try {
            if (requestObserver != null) {
                try {
                    requestObserver.onCompleted();
                } catch (RuntimeException ignored) {
                    // stream may already be torn down
                }
                requestObserver = null;
            }
        } finally {
            streamLock.unlock();
        }
        failAllPending("venue route client shutting down", new IllegalStateException("shutdown"));
        if (routeTimeoutScheduler != null) {
            routeTimeoutScheduler.shutdownNow();
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
