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

import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "internal-venue")
public class VenueRouteOrderClient {

    private static final Logger log = LoggerFactory.getLogger(VenueRouteOrderClient.class);

    private final ManagedChannel channel;
    private final VenueOrderServiceGrpc.VenueOrderServiceBlockingStub blockingStub;
    private final VenueOrderServiceGrpc.VenueOrderServiceStub asyncStub;
    private final long grpcCallTimeoutMs;
    private final MeterRegistry meterRegistry;

    // ---- Pipelined RouteOrderStream state (used only when venue-route-max-in-flight > 1) ----
    /** In-flight async routes keyed by oms order id; completed by the response demuxer. */
    private final ConcurrentHashMap<String, PendingRoute> pendingRoutes = new ConcurrentHashMap<>();
    /** Guards stream (re)open + ordered writes to {@link #requestObserver}. */
    private final ReentrantLock streamLock = new ReentrantLock();
    private volatile StreamObserver<RouteOrderRequest> requestObserver;
    private volatile boolean shuttingDown;
    private ScheduledExecutorService routeTimeoutScheduler;

    private record PendingRoute(
            CompletableFuture<Optional<ExecutionReport>> future, ScheduledFuture<?> timeout) {}

    public VenueRouteOrderClient(OmsConfig omsConfig, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.grpcCallTimeoutMs = omsConfig.getVenue().getGrpcCallTimeoutMs();
        this.channel =
                ManagedChannelBuilder.forAddress(
                                omsConfig.getVenue().getGrpcHost(), omsConfig.getVenue().getGrpcPort())
                        .usePlaintext()
                        .build();
        this.blockingStub = VenueOrderServiceGrpc.newBlockingStub(channel);
        this.asyncStub = VenueOrderServiceGrpc.newStub(channel);
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
     * Pipelined route over the ordered {@code RouteOrderStream}: writes the request in the caller's
     * invocation order (the egress replay thread calls this in cluster-log order, so the venue sees
     * offers in admission order and price-time priority is preserved) and returns a future the
     * response demuxer completes when the venue acks. {@link Optional#empty()} means the venue
     * rejected / did not (fully) accept; a transport/stream failure completes the future
     * exceptionally with {@link VenueRouteTransportException} so the egress submits a VENUE_REJECT,
     * exactly as the blocking path does. Not thread-safe across callers — the single egress replay
     * thread is the only writer.
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
        streamLock.lock();
        try {
            ScheduledFuture<?> timeout =
                    routeTimeoutScheduler().schedule(
                            () -> {
                                PendingRoute pr = pendingRoutes.remove(orderId);
                                if (pr != null) {
                                    pr.future().completeExceptionally(
                                            new VenueRouteTransportException(
                                                    "venue RouteOrderStream ack timeout orderId=" + orderId,
                                                    new java.util.concurrent.TimeoutException("ack timeout")));
                                }
                            },
                            grpcCallTimeoutMs,
                            MILLISECONDS);
            PendingRoute prev = pendingRoutes.putIfAbsent(orderId, new PendingRoute(future, timeout));
            if (prev != null) {
                timeout.cancel(false);
                future.completeExceptionally(
                        new VenueRouteTransportException(
                                "duplicate in-flight venue route orderId=" + orderId, new IllegalStateException()));
                return future;
            }
            try {
                ensureStreamOpenLocked();
                requestObserver.onNext(request);
            } catch (RuntimeException e) {
                PendingRoute pr = pendingRoutes.remove(orderId);
                if (pr != null) {
                    pr.timeout().cancel(false);
                }
                OmsVenueGrpcMetrics.recordEgressFailure(meterRegistry, OmsVenueGrpcMetrics.RPC_ROUTE_ORDER, asStatusRuntime(e));
                future.completeExceptionally(
                        new VenueRouteTransportException(
                                "venue RouteOrderStream write failed orderId=" + orderId, e));
            }
        } finally {
            streamLock.unlock();
        }
        return future;
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

    /** Demuxes async venue responses to the matching in-flight future by oms order id. */
    private final class RouteStreamResponseObserver implements StreamObserver<RouteOrderResponse> {
        @Override
        public void onNext(RouteOrderResponse response) {
            PendingRoute pr = pendingRoutes.remove(response.getOmsOrderId());
            if (pr == null) {
                return; // late/duplicate (e.g. after a timeout already fired)
            }
            pr.timeout().cancel(false);
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
        List<String> ids = new ArrayList<>(pendingRoutes.keySet());
        for (String id : ids) {
            PendingRoute pr = pendingRoutes.remove(id);
            if (pr != null) {
                pr.timeout().cancel(false);
                pr.future().completeExceptionally(
                        new VenueRouteTransportException(message + " orderId=" + id, cause));
            }
        }
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
