package com.balh.oms.ingress;

import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrdersRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Internal admin surface for order-state hygiene that bypasses the broker round-trip.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@link OrderLifecycleController}'s {@code POST /{id}/cancel} translates to a
 * {@link com.balh.oms.cluster.RequestCancelOrderCommand}, which the FIX path then turns into a
 * 35=F OrderCancelRequest on the wire. That is the right path for normal operator-initiated
 * cancels. It is the <strong>wrong</strong> path when an order is stuck because the broker is
 * deliberately refusing cancels — the demo simulator's {@code CXLREJ} symbol trigger is the
 * obvious case, but production has the same shape any time a venue rejects 35=F for compliance
 * or symbol-status reasons. Sending more 35=F doesn't help; the order needs to be marked
 * terminal locally with audit.
 *
 * <h2>How it works</h2>
 *
 * <p>This endpoint reuses the existing {@link com.balh.oms.cluster.CancelOrderCommand} pipeline
 * that {@code LedgerInflightHoldFailureCompensator} (slice 4p) uses to compensate a failed
 * buying-power hold without a broker round-trip:
 *
 * <ol>
 *   <li>Submit a {@code CancelOrderCommand} via {@link OmsClusterIngressClient#submitCancelOrder}.</li>
 *   <li>Cluster's {@link com.balh.oms.cluster.OmsAdmissionClusteredService} apply path mutates
 *       status to {@code CANCELLED}, bumps version, emits one
 *       {@link com.balh.oms.cluster.OrderCancelAppliedEvent} on the side publication.</li>
 *   <li>Projector consumes the event, writes {@code orders.status = CANCELLED} and the matching
 *       {@code domain_event_outbox} envelope. No {@code executions} row (this cancel never
 *       touched a venue).</li>
 *   <li>NATS publisher picks the envelope up; customer-frontend and trading-desk see the row
 *       flip to {@code CANCELLED} on the next push.</li>
 * </ol>
 *
 * <p>Reuses the cluster command, so cluster log replay re-applies the cancel idempotently.
 * Already-terminal orders are a silent no-op in the cluster — the controller's 409 pre-check
 * just gives the operator a faster, more honest error response.
 *
 * <h2>Production caveat</h2>
 *
 * <p>The broker still believes the order is open. If the venue later fills it the inbound ER
 * will hit a terminal local order; the cluster apply path will reject the cumQty advance
 * (already-CANCELLED → no state transition). Operators using this endpoint in production
 * <strong>must</strong> follow up with an out-of-band broker-side cancel (phone, broker
 * dashboard, etc.) for any order against a real venue. The endpoint is safe-by-default for the
 * stub simulator since the simulator never fills resting LIMITs unsolicited.
 *
 * <h2>Auth</h2>
 *
 * <p>Same as every other {@code /internal/v1/**} endpoint — {@link ApiKeyFilter} requires
 * {@code X-OMS-Internal-Key} matching {@code OMS_HTTP_INTERNAL_API_KEY}. There is no
 * per-actor auth on this surface today; the audit log line is the only forensic record. A
 * future slice can add {@code X-Admin-Actor} or wire the trading-desk BFF's authenticated user
 * through.
 */
@RestController
@RequestMapping("/internal/v1/admin/orders/{orderId}")
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class AdminOrderController {

    private static final Logger log = LoggerFactory.getLogger(AdminOrderController.class);

    /**
     * Conservative cap on the operator-supplied reason. {@link CancelOrderCommand} bounds the
     * field at {@link com.balh.oms.cluster.OmsClusterWireFormat#MAX_STRING_BYTES} bytes after
     * UTF-8 encoding; this leaves room for the {@link #REASON_PREFIX} and any multibyte trailer.
     */
    public static final int MAX_REASON_LEN = 240;

    /**
     * Marker on the cluster command's reason field so cluster log readers and the projector's
     * {@code domain_event_outbox} consumers can recognise that this cancel came from the admin
     * endpoint and not from a broker round-trip or the inflight compensator.
     */
    public static final String REASON_PREFIX = "admin-force-cancel: ";

    private final OmsConfig config;
    private final OmsClusterIngressClient cluster;
    private final OrdersRepository orders;
    private final Counter requestedCounter;
    private final Counter alreadyTerminalCounter;

    public AdminOrderController(
            OmsConfig config,
            OmsClusterIngressClient cluster,
            OrdersRepository orders,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.cluster = cluster;
        this.orders = orders;
        this.requestedCounter = Counter.builder("oms_orders_admin_force_cancel_requested_total")
                .description("Admin force-cancel commands submitted to the cluster via HTTP")
                .register(meterRegistry);
        this.alreadyTerminalCounter = Counter.builder("oms_orders_admin_force_cancel_skipped_total")
                .description("Admin force-cancel requests skipped because order is already terminal")
                .register(meterRegistry);
    }

    public record ForceCancelRequestBody(String reason) {}

    public record AdminResponse(String status, String orderStatus, String message) {}

    @PostMapping("/force-cancel")
    public ResponseEntity<?> forceCancel(
            @PathVariable UUID orderId,
            @Valid @RequestBody(required = false) ForceCancelRequestBody body) {

        var orderOpt = orders.findById(orderId);
        if (orderOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "order_not_found"));
        }
        var order = orderOpt.get();
        if (order.status().isTerminal()) {
            alreadyTerminalCounter.increment();
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "error", "order_already_terminal",
                            "orderStatus", order.status().name()));
        }

        String rawReason = body == null ? null : body.reason();
        String reason = REASON_PREFIX + trimToMax(rawReason, MAX_REASON_LEN);

        Duration timeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        long correlationId = cluster.nextCorrelationId();
        CancelOrderCommand cmd = new CancelOrderCommand(
                correlationId, orderId, System.nanoTime(), reason);
        try {
            cluster.submitCancelOrder(cmd, timeout);
        } catch (TimeoutException e) {
            log.warn(
                    "admin force-cancel: cluster submit timed out orderId={} correlationId={}",
                    orderId, correlationId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_timeout"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_submit_interrupted"));
        } catch (IllegalStateException e) {
            log.warn(
                    "admin force-cancel: cluster not connected orderId={}",
                    orderId, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "cluster_unavailable"));
        }
        requestedCounter.increment();
        log.info(
                "admin force-cancel submitted orderId={} status={} correlationId={} reason={}",
                orderId, order.status().name(), correlationId, reason);
        return ResponseEntity.accepted()
                .body(new AdminResponse(
                        "force_cancel_requested",
                        order.status().name(),
                        "Force-cancel submitted to cluster; projector will flip status to CANCELLED."));
    }

    private static String trimToMax(String value, int max) {
        if (value == null) {
            return "";
        }
        String t = value.trim();
        return t.length() > max ? t.substring(0, max) : t;
    }
}
