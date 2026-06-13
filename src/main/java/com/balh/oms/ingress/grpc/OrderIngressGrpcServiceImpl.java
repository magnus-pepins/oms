package com.balh.oms.ingress.grpc;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import com.balh.oms.ingress.LedgerBindingException;
import com.balh.oms.ingress.OrderIngressService;
import com.balh.oms.proto.ingress.v1.CreateOrderResponse;
import com.balh.oms.proto.ingress.v1.OrderIngressGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * gRPC ingress for order accept (same transaction semantics as HTTP {@code POST /internal/v1/orders}).
 * Auth: {@link GrpcInternalApiKeyInterceptor} on {@code x-oms-internal-key} metadata.
 */
@Service
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderIngressGrpcServiceImpl extends OrderIngressGrpc.OrderIngressImplBase {

    private final OrderIngressService ingress;
    private final Validator validator;

    public OrderIngressGrpcServiceImpl(OrderIngressService ingress, Validator validator) {
        this.ingress = ingress;
        this.validator = validator;
    }

    @Override
    public void createOrder(
            com.balh.oms.proto.ingress.v1.CreateOrderRequest request,
            StreamObserver<CreateOrderResponse> responseObserver) {
        try {
            com.balh.oms.ingress.CreateOrderRequest domain = mapRequest(request);
            Set<ConstraintViolation<com.balh.oms.ingress.CreateOrderRequest>> violations = validator.validate(domain);
            if (!violations.isEmpty()) {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription(violations.iterator().next().getMessage())
                                .asRuntimeException());
                return;
            }
            OrderIngressService.IngressResult result = ingress.persistAccepted(domain);
            responseObserver.onNext(toProtoResponse(result.order(), result.created()));
            responseObserver.onCompleted();
        } catch (LedgerBindingException e) {
            responseObserver.onError(mapLedgerStatus(e).withDescription(e.getMessage()).asRuntimeException());
        } catch (RuntimeException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private static Status mapLedgerStatus(LedgerBindingException e) {
        return switch (e.getHttpStatus()) {
            case BAD_REQUEST -> Status.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.NOT_FOUND;
            case BAD_GATEWAY -> Status.UNAVAILABLE;
            default -> Status.INVALID_ARGUMENT;
        };
    }

    private static com.balh.oms.ingress.CreateOrderRequest mapRequest(com.balh.oms.proto.ingress.v1.CreateOrderRequest g) {
        String lp = g.getLimitPrice().trim();
        BigDecimal limitPrice = lp.isEmpty() ? null : new BigDecimal(lp);
        return new com.balh.oms.ingress.CreateOrderRequest(
                UUID.fromString(g.getAccountId().trim()),
                g.getClientIdempotencyKey().trim(),
                Side.valueOf(g.getSide().trim().toUpperCase(Locale.ROOT)),
                g.getInstrumentSymbol().trim(),
                new BigDecimal(g.getQuantity().trim()),
                limitPrice,
                g.getTimeInForce().trim(),
                /* orderType = */ null,
                blankToNull(g.getLedgerBalanceId()),
                blankToNull(g.getLedgerIdentityId()),
                /* fxQuoteId = */ null,
                /* cashHoldAmount = */ null,
                blankToNull(g.getPortfolioId()));
    }

    private static String blankToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static CreateOrderResponse toProtoResponse(Order o, boolean created) {
        CreateOrderResponse.Builder b = CreateOrderResponse.newBuilder()
                .setId(o.id().toString())
                .setAccountId(o.accountId().toString())
                .setClientIdempotencyKey(o.clientIdempotencyKey())
                .setShardId(o.shardId())
                .setVersion(o.version())
                .setStatus(o.status().name())
                .setSide(o.side().name())
                .setInstrumentSymbol(o.instrumentSymbol())
                .setQuantity(o.quantity().toPlainString())
                .setLimitPrice(o.limitPrice() == null ? "" : o.limitPrice().toPlainString())
                .setTimeInForce(o.timeInForce())
                .setLedgerBalanceId(o.ledgerBalanceId() == null ? "" : o.ledgerBalanceId())
                .setSettlementStatus("")
                .setCreated(created);
        if (o.receivedAt() != null) {
            b.setReceivedAt(instantToTs(o.receivedAt()));
        }
        if (o.acceptedAt() != null) {
            b.setAcceptedAt(instantToTs(o.acceptedAt()));
        }
        if (o.terminalReason() != null) {
            b.setTerminalReason(o.terminalReason().name());
        }
        if (o.terminalAt() != null) {
            b.setTerminalAt(instantToTs(o.terminalAt()));
        }
        return b.build();
    }

    private static Timestamp instantToTs(java.time.Instant i) {
        return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
    }
}
