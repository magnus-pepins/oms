package com.balh.oms.ingress;

import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
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
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
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
 * <h2>Cluster-forgot-order observation</h2>
 *
 * <p>The cluster's {@link com.balh.oms.cluster.OmsAdmissionClusteredService#applyCancelOrder
 * applyCancelOrder} silently returns if its in-memory {@code orderIndex} does not contain the
 * target order id — the original intent was "ignore duplicate cancels for terminal orders that
 * have already been evicted from the index". The corner case that breaks the operator's mental
 * model is when the Postgres {@code orders} row exists in a non-terminal state but the cluster
 * has lost the in-memory mapping (typical cause: cluster journal wipe + replay that did not
 * restore the order, e.g. after a slice deploy that truncated the journal). The controller
 * used to return {@code 202 Accepted} in that case, and the operator was left believing the
 * cancel was queued when in fact nothing was going to happen.
 *
 * <p>Fix: after the cluster submit returns OK, poll the orders row for up to
 * {@link OmsConfig.Admin#getCancelObservationTimeoutMs()} ms (default 2000) for a version bump
 * combined with a terminal status — that pair uniquely identifies "projector applied an
 * {@link com.balh.oms.cluster.OrderCancelAppliedEvent} for our submit (or for an earlier one
 * we lost the race with — either way, the operator-visible state is correct)". If the
 * deadline passes without an observed bump, return {@code 410 Gone} with
 * {@code cluster_forgot_order} so the caller knows the cluster swallowed it and manual
 * Postgres cleanup is required.
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

    /**
     * Audit marker in logs for {@link #forceMarkCancelledPostgresOnly(UUID, ForceCancelRequestBody)}.
     * Does not write to {@code orders.terminal_reason} (reject_code enum); forensic trail is log + metric.
     */
    public static final String POSTGRES_ONLY_CANCEL_LOG_MARKER = "admin-postgres-only-cancel";

    private final OmsConfig config;
    private final OmsClusterIngressClient cluster;
    private final OrdersRepository orders;
    private final Counter requestedCounter;
    private final Counter alreadyTerminalCounter;
    private final Counter clusterForgotCounter;
    private final Counter appliedCounter;
    private final Counter postgresOnlyAppliedCounter;
    private final Counter postgresOnlySkippedCounter;
    private final Counter postgresOnlyConflictCounter;

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
        this.clusterForgotCounter = Counter.builder("oms_orders_admin_force_cancel_cluster_forgot_total")
                .description("Admin force-cancel requests where the cluster accepted the command but produced no apply event within the observation timeout (orderIndex miss, likely post-journal-wipe)")
                .register(meterRegistry);
        this.appliedCounter = Counter.builder("oms_orders_admin_force_cancel_applied_total")
                .description("Admin force-cancel requests where the projector observed the cancel apply within the observation timeout")
                .register(meterRegistry);
        this.postgresOnlyAppliedCounter =
                Counter.builder("oms_orders_admin_force_mark_cancelled_postgres_applied_total")
                        .description("Postgres-only admin cancels (journal-loss hygiene; no cluster event)")
                        .register(meterRegistry);
        this.postgresOnlySkippedCounter =
                Counter.builder("oms_orders_admin_force_mark_cancelled_postgres_skipped_total")
                        .description("Postgres-only admin cancel skipped (already terminal)")
                        .register(meterRegistry);
        this.postgresOnlyConflictCounter =
                Counter.builder("oms_orders_admin_force_mark_cancelled_postgres_conflict_total")
                        .description("Postgres-only admin cancel CAS lost (concurrent writer)")
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
        // Captured before the cluster submit so the post-submit observation loop can detect a
        // version bump produced by the projector applying our cancel (or any other writer's
        // cancel that won the race — operator-visible outcome is the same in both cases).
        int versionAtSubmit = order.version();

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
                "admin force-cancel submitted orderId={} status={} version={} correlationId={} reason={}",
                orderId, order.status().name(), versionAtSubmit, correlationId, reason);

        Optional<Order> applied = pollForCancelApplied(orderId, versionAtSubmit);
        if (applied.isPresent()) {
            appliedCounter.increment();
            Order observed = applied.get();
            log.info(
                    "admin force-cancel applied orderId={} status={} version={} correlationId={}",
                    orderId, observed.status().name(), observed.version(), correlationId);
            return ResponseEntity.ok(new AdminResponse(
                    "force_cancel_applied",
                    observed.status().name(),
                    "Cluster applied cancel; projector wrote status=" + observed.status().name() + "."));
        }

        // Cluster received the submit, but the projector never observed an apply event. The
        // canonical cause is OmsAdmissionClusteredService.applyCancelOrder silently no-op'ing
        // because the in-memory orderIndex did not contain this orderId (typically after a
        // cluster journal wipe + replay that did not restore the order). 410 Gone communicates
        // "the cluster knows nothing about this order; the Postgres row is orphaned from the
        // cluster's perspective" — operator needs out-of-band Postgres cleanup.
        clusterForgotCounter.increment();
        long observationTimeoutMs = config.getAdmin().getCancelObservationTimeoutMs();
        log.warn(
                "admin force-cancel: cluster swallowed command — no apply event observed within {} ms"
                        + " orderId={} status={} version={} correlationId={}."
                        + " Cluster orderIndex likely missing this order (post-journal-wipe?);"
                        + " operator must update Postgres orders row directly to mark it terminal.",
                observationTimeoutMs, orderId, order.status().name(), versionAtSubmit, correlationId);
        return ResponseEntity.status(HttpStatus.GONE).body(Map.of(
                "error", "cluster_forgot_order",
                "orderStatus", order.status().name(),
                "observationTimeoutMs", observationTimeoutMs,
                "message",
                "Cluster accepted the command but produced no cancel-applied event within "
                        + observationTimeoutMs
                        + " ms. The cluster has likely lost this order from its in-memory state"
                        + " (e.g. after a journal wipe). The Postgres orders row is orphaned —"
                        + " manual cleanup required."));
    }

    /**
     * Postgres-only terminal transition for orders the cluster no longer knows about (typical:
     * journal wipe). Does <strong>not</strong> submit a {@link CancelOrderCommand} and does
     * <strong>not</strong> enqueue {@code domain_event_outbox} — UIs may need refresh; broker
     * may still believe the order is open on a real venue.
     *
     * <p>Prefer {@link #forceCancel(UUID, ForceCancelRequestBody)} first; use this endpoint when
     * that returns {@code 410 Gone} {@code cluster_forgot_order}.
     */
    @PostMapping("/force-mark-cancelled-postgres-only")
    public ResponseEntity<?> forceMarkCancelledPostgresOnly(
            @PathVariable UUID orderId,
            @Valid @RequestBody(required = false) ForceCancelRequestBody body) {

        var orderOpt = orders.findById(orderId);
        if (orderOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "order_not_found"));
        }
        var order = orderOpt.get();
        if (order.status().isTerminal()) {
            postgresOnlySkippedCounter.increment();
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "error", "order_already_terminal",
                            "orderStatus", order.status().name()));
        }

        String operatorNote = body == null ? null : body.reason();
        Instant terminalAt = Instant.now();
        boolean applied =
                orders.updateWithCas(orderId, order.version(), OrderStatus.CANCELLED, null, null, terminalAt);
        if (!applied) {
            postgresOnlyConflictCounter.increment();
            log.warn(
                    "{} CAS lost orderId={} expectedVersion={} status={} operatorNote={}",
                    POSTGRES_ONLY_CANCEL_LOG_MARKER,
                    orderId,
                    order.version(),
                    order.status().name(),
                    trimToMax(operatorNote, MAX_REASON_LEN));
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "version_conflict", "orderStatus", order.status().name()));
        }

        postgresOnlyAppliedCounter.increment();
        var updated = orders.findById(orderId).orElse(order);
        log.warn(
                "{} applied orderId={} priorStatus={} priorVersion={} newStatus={} newVersion={} terminalAt={} operatorNote={}"
                        + " — Postgres mirror only; cluster and domain_event_outbox unchanged",
                POSTGRES_ONLY_CANCEL_LOG_MARKER,
                orderId,
                order.status().name(),
                order.version(),
                updated.status().name(),
                updated.version(),
                terminalAt,
                trimToMax(operatorNote, MAX_REASON_LEN));
        return ResponseEntity.ok(new AdminResponse(
                "postgres_cancel_applied",
                updated.status().name(),
                "Orders row marked CANCELLED in Postgres only. Cluster state and NATS fanout unchanged."
                        + " Follow broker out-of-band cancel on real venues if applicable."));
    }

    /**
     * Polls the orders row for up to {@link OmsConfig.Admin#getCancelObservationTimeoutMs()} ms
     * waiting for the projector to apply a cancel event — detected by a version bump and a
     * terminal status. Returns the observed order when seen, or empty if the deadline passes.
     */
    private Optional<Order> pollForCancelApplied(UUID orderId, int versionAtSubmit) {
        long timeoutMs = config.getAdmin().getCancelObservationTimeoutMs();
        long pollMs = config.getAdmin().getCancelObservationPollIntervalMs();
        long deadlineNanos = System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
        while (true) {
            var current = orders.findById(orderId);
            if (current.isPresent()
                    && current.get().version() > versionAtSubmit
                    && current.get().status().isTerminal()) {
                return current;
            }
            if (System.nanoTime() >= deadlineNanos) {
                return Optional.empty();
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
    }

    private static String trimToMax(String value, int max) {
        if (value == null) {
            return "";
        }
        String t = value.trim();
        return t.length() > max ? t.substring(0, max) : t;
    }
}
