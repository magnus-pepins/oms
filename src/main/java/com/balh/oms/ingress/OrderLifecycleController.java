package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.RequestCancelOrderCommand;
import com.balh.oms.cluster.RequestReplaceOrderCommand;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrdersRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Wed-demo HTTP surface for user-initiated cancel + modify. Routes user requests through the
 * cluster (fire-and-forget submit) and returns 202 Accepted — the broker round-trip is
 * asynchronous, the cluster emits the {@code OrderCancelRequested} / {@code OrderReplaceRequested}
 * event, oms-fix-egress turns it into 35=F / 35=G, and only the broker's subsequent execution
 * report (ET=4 / ET=5) or 35=9 OrderCancelReject changes the order's terminal/working state.
 *
 * <p>Two separate endpoints rather than a single PATCH because:
 * <ul>
 *   <li>Cancel and replace map to <strong>distinct cluster commands</strong>
 *       ({@link RequestCancelOrderCommand} vs {@link RequestReplaceOrderCommand}) with different
 *       payload shapes and idempotency-key prefixes. A single PATCH would have to dispatch on
 *       the body shape, which is more error-prone for both client and server.</li>
 *   <li>{@code POST /{id}/cancel} and {@code POST /{id}/replace} are the standard REST shape for
 *       "trigger a non-CRUD lifecycle action", and that's what both the customer-frontend BFF
 *       and the trading-desk BFF will call.</li>
 * </ul>
 *
 * <h2>Idempotency</h2>
 *
 * <p>The {@code clientRequestKey} (read from request body, with {@code Idempotency-Key} header
 * as a fallback) is forwarded to the cluster, which dedupes per-order on
 * {@code ('c' + key)} / {@code ('r' + key)}. A retried HTTP request that lands on the same
 * cluster command instance is a no-op; a retried request that arrives <em>after</em> the first
 * cluster commit is dropped by the same dedupe set. Either way, the broker sees at most one
 * 35=F (or 35=G) per {@code (orderId, clientRequestKey)} tuple — the FIX builders derive their
 * outbound {@code ClOrdID(11)} from the same key, so even cluster-log replay collisions resolve
 * via the broker's {@code DupClOrdID} reject.
 *
 * <h2>Status reporting</h2>
 *
 * <p>The endpoints return 202 with the order's <strong>current</strong> projected status after
 * a brief read of the orders row (still WORKING / PARTIALLY_FILLED right after admission — the
 * UI is expected to render a "cancel requested" / "modify requested" badge until a NATS
 * envelope or polled status change flips it). 404 if the order doesn't exist; 409 if it's
 * already terminal (no broker call possible).
 */
@RestController
@RequestMapping("/internal/v1/orders/{orderId}")
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderLifecycleController {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleController.class);

    public static final int MAX_CLIENT_REQUEST_KEY_LEN = 128;
    public static final int MAX_REASON_LEN = 256;
    private static final BigDecimal QUANTITY_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE);
    private static final BigDecimal PRICE_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE);

    private final OmsConfig config;
    private final OmsClusterIngressClient cluster;
    private final OrdersRepository orders;
    private final Counter cancelRequestedCounter;
    private final Counter replaceRequestedCounter;

    public OrderLifecycleController(
            OmsConfig config,
            OmsClusterIngressClient cluster,
            OrdersRepository orders,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.cluster = cluster;
        this.orders = orders;
        this.cancelRequestedCounter = Counter.builder("oms_orders_cancel_requested_total")
                .description("Cancel requests submitted to the cluster via HTTP")
                .register(meterRegistry);
        this.replaceRequestedCounter = Counter.builder("oms_orders_replace_requested_total")
                .description("Replace requests submitted to the cluster via HTTP")
                .register(meterRegistry);
    }

    public record CancelRequestBody(String clientRequestKey, String reason) {}

    public record ReplaceRequestBody(
            String clientRequestKey,
            @NotNull BigDecimal newQuantity,
            BigDecimal newLimitPrice,
            String reason) {}

    public record LifecycleResponse(String status, String orderStatus, String message) {}

    @PostMapping("/cancel")
    public ResponseEntity<?> cancel(
            @PathVariable UUID orderId,
            @Valid @RequestBody(required = false) CancelRequestBody body,
            @RequestParam(name = "idempotencyKey", required = false) String idempotencyKeyParam) {

        var orderOpt = orders.findById(orderId);
        if (orderOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "order_not_found"));
        }
        var order = orderOpt.get();
        if (order.status().isTerminal()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "error", "order_already_terminal",
                            "orderStatus", order.status().name()));
        }

        String requestKey = resolveRequestKey(
                body == null ? null : body.clientRequestKey(), idempotencyKeyParam, "cancel", orderId);
        String reason = trimToMax(body == null ? null : body.reason(), MAX_REASON_LEN);

        Duration timeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        RequestCancelOrderCommand cmd = new RequestCancelOrderCommand(
                cluster.nextCorrelationId(),
                orderId,
                System.nanoTime(),
                requestKey,
                reason);
        try {
            cluster.submitRequestCancelOrder(cmd, timeout);
        } catch (TimeoutException e) {
            log.warn(
                    "submitRequestCancelOrder back-pressure timeout orderId={} requestKey={}",
                    orderId, requestKey, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_timeout"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_interrupted"));
        } catch (IllegalStateException e) {
            log.warn("submitRequestCancelOrder cluster not connected orderId={}", orderId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_unavailable"));
        }
        cancelRequestedCounter.increment();
        return ResponseEntity.accepted()
                .body(new LifecycleResponse("cancel_requested", order.status().name(),
                        "Cancel request submitted; broker round-trip pending."));
    }

    @PostMapping("/replace")
    public ResponseEntity<?> replace(
            @PathVariable UUID orderId,
            @Valid @RequestBody ReplaceRequestBody body,
            @RequestParam(name = "idempotencyKey", required = false) String idempotencyKeyParam) {

        if (body == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_body"));
        }
        if (body.newQuantity() == null || body.newQuantity().signum() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "new_quantity_must_be_positive"));
        }
        if (body.newLimitPrice() != null && body.newLimitPrice().signum() < 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "new_limit_price_negative"));
        }

        var orderOpt = orders.findById(orderId);
        if (orderOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "order_not_found"));
        }
        var order = orderOpt.get();
        if (order.status().isTerminal()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "error", "order_already_terminal",
                            "orderStatus", order.status().name()));
        }

        long newQtyScaled = scaleOrThrow(body.newQuantity(), QUANTITY_SCALE_BD, "new_quantity");
        long newPriceScaled = body.newLimitPrice() == null
                ? 0L
                : scaleOrThrow(body.newLimitPrice(), PRICE_SCALE_BD, "new_limit_price");

        String requestKey = resolveRequestKey(
                body.clientRequestKey(), idempotencyKeyParam, "replace", orderId);
        String reason = trimToMax(body.reason(), MAX_REASON_LEN);

        Duration timeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        RequestReplaceOrderCommand cmd = new RequestReplaceOrderCommand(
                cluster.nextCorrelationId(),
                orderId,
                newQtyScaled,
                newPriceScaled,
                System.nanoTime(),
                requestKey,
                reason);
        try {
            cluster.submitRequestReplaceOrder(cmd, timeout);
        } catch (TimeoutException e) {
            log.warn(
                    "submitRequestReplaceOrder back-pressure timeout orderId={} requestKey={}",
                    orderId, requestKey, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_timeout"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_interrupted"));
        } catch (IllegalStateException e) {
            log.warn("submitRequestReplaceOrder cluster not connected orderId={}", orderId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_unavailable"));
        }
        replaceRequestedCounter.increment();
        return ResponseEntity.accepted()
                .body(new LifecycleResponse("replace_requested", order.status().name(),
                        "Replace request submitted; broker round-trip pending."));
    }

    /**
     * Body-key wins, then header param, then a synthesized fallback. Empty body-key falls through
     * the same precedence so an explicit empty string in the body opts into the synthesized key
     * (rather than turning off dedupe entirely, which would surprise the broker).
     */
    private static String resolveRequestKey(
            String bodyKey, String headerKey, String op, UUID orderId) {
        String chosen = (bodyKey != null && !bodyKey.isBlank()) ? bodyKey
                : (headerKey != null && !headerKey.isBlank()) ? headerKey
                : orderId + ":" + op + ":fallback";
        return trimToMax(chosen, MAX_CLIENT_REQUEST_KEY_LEN);
    }

    private static String trimToMax(String value, int max) {
        if (value == null) {
            return "";
        }
        String t = value.trim();
        return t.length() > max ? t.substring(0, max) : t;
    }

    /**
     * Scales a decimal value by the corresponding fixed-point scale, rejecting fractional input
     * the scale cannot represent. Mirrors {@code OrderIngressService}'s validation so the
     * cluster wire never sees a quantity / price that lost precision in HTTP-edge conversion.
     */
    private static long scaleOrThrow(BigDecimal value, BigDecimal scale, String field) {
        try {
            return value.multiply(scale).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
        } catch (ArithmeticException e) {
            throw new BadInputException(field + "_invalid_scale", e);
        }
    }

    /**
     * Tunneling exception that the Spring exception handler translates to 400 Bad Request.
     * Keeps the scale-validation error path symmetric with the explicit early returns above
     * (caller doesn't have to thread {@code throws} for what is logically a 400-shaped error).
     */
    static class BadInputException extends RuntimeException {
        private final String errorCode;

        BadInputException(String errorCode, Throwable cause) {
            super(errorCode, cause);
            this.errorCode = errorCode;
        }

        public String errorCode() {
            return errorCode;
        }
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(BadInputException.class)
    public ResponseEntity<?> handleBadInput(BadInputException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.errorCode()));
    }
}
