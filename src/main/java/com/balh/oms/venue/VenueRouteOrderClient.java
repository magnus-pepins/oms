package com.balh.oms.venue;

import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.venue.grpc.v1.ExecutionReport;
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "internal-venue")
public class VenueRouteOrderClient {

    private static final Logger log = LoggerFactory.getLogger(VenueRouteOrderClient.class);

    private final ManagedChannel channel;
    private final VenueOrderServiceGrpc.VenueOrderServiceBlockingStub blockingStub;
    private final long grpcCallTimeoutMs;

    public VenueRouteOrderClient(OmsConfig omsConfig) {
        this.grpcCallTimeoutMs = omsConfig.getVenue().getGrpcCallTimeoutMs();
        this.channel =
                ManagedChannelBuilder.forAddress(
                                omsConfig.getVenue().getGrpcHost(), omsConfig.getVenue().getGrpcPort())
                        .usePlaintext()
                        .build();
        this.blockingStub = VenueOrderServiceGrpc.newBlockingStub(channel);
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
            throw new VenueRouteTransportException(
                    "venue RouteReplace transport failure orderId=" + ev.orderId() + " symbol=" + ev.instrumentSymbol(),
                    e);
        }
        return acceptedExecutionReport(response.getAccepted(), response.getRejectReason(), ev.orderId(), response.hasExecutionReport(), response.getExecutionReport());
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
