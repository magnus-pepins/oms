package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.ShardKey;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightCoalescer;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Drives mandatory admission through Aeron Cluster on every order accept, and (for now) records
 * the residual outbox / fanout side-tables that downstream Phase 2 slices will retire.
 *
 * <p>Phase 1c slice B (Aeron Cluster substrate plan) made the cluster the only admission path.
 * {@link OmsClusterIngressClient} is a hard required dependency: every accept call submits an
 * {@link AcceptOrderCommand} and blocks until the cluster emits {@code OrderAccepted} or
 * {@code OrderRejected}.
 *
 * <p>Phase 2 slice 2c removed the {@code orders} INSERT from the ingress transaction.
 * {@code orders} is a downstream projection of the cluster log, written by the
 * {@code oms-postgres-projector} JVM (see {@link com.balh.oms.projector.OmsPostgresProjector}
 * and the V25 migration that drops the FK constraints back to {@code orders(id)}). HTTP 200/201
 * means cluster-committed; the orders row appears in Postgres after the projector applies the
 * cluster-emitted {@code OrderAdmittedEvent} (sub-millisecond in steady state). Tests that
 * previously read the orders row immediately after the controller response upgrade to
 * Awaitility, and the {@code INGRESS} control-Postgres-write-path branch is gone — slice 2d
 * subsumes its work into the projector.
 *
 * <p>Phase 3 slice 3f drops the {@code control_outbox} insert too: the projector emits
 * {@code OrderWorking} into {@code domain_event_outbox} directly from {@code OrderAdmittedEvent}
 * (slice 2d), and the {@code oms-fix-egress} JVM drives FIX outbound from the cluster's events
 * recording (slices 3a–3d), so no row in {@code control_outbox} ever has a downstream consumer.
 *
 * <p>Phase 4 Tier 2.5 phase D-3 retired the {@code domain_event_outbox.insert(OrderAccepted)}
 * from this method too: the projector emits the {@code OrderAccepted} envelope from the
 * cluster's {@link com.balh.oms.cluster.OrderAdmittedEvent} (gated on the
 * {@code orders} ON-CONFLICT-DO-NOTHING insert returning a fresh row, mirroring the existing
 * idempotency for {@code OrderWorking}). The only Postgres write that may still happen on
 * the ingress hot path is the optional BUY-async {@code ledger_inflight_outbox} insert
 * (slice 4p / D-1), which a single auto-committing JDBC statement can do without a Spring
 * transaction — Hikari's default {@code autoCommit=true} commits per-statement.
 *
 * <p>Net effect: SELL and BUY-without-inflight orders open <strong>zero</strong> Postgres
 * connections on the hot path; BUY-with-inflight-async opens one connection just long
 * enough for the single INSERT (sub-millisecond). The {@link
 * org.springframework.transaction.support.TransactionTemplate} that D-1 left in place is
 * gone — there is nothing to demarcate.
 */
@Service
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderIngressService {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressService.class);

    /** Micrometer name for Ledger sync inflight hold latency (tag {@code result}). */
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private final OrdersRepository orders;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final PiiHash piiHash;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;
    private final LedgerInflightOutboxRepository ledgerInflightOutbox;
    private final MeterRegistry meterRegistry;
    private final OrderControlAdmission orderControlAdmission;
    private final OmsClusterIngressClient clusterIngressClient;

    public OrderIngressService(
            OrdersRepository orders,
            OmsConfig config,
            ObjectMapper objectMapper,
            PiiHash piiHash,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            LedgerInflightOutboxRepository ledgerInflightOutbox,
            MeterRegistry meterRegistry,
            OrderControlAdmission orderControlAdmission,
            OmsClusterIngressClient clusterIngressClient) {
        this.orders = orders;
        this.config = config;
        this.objectMapper = objectMapper;
        this.piiHash = piiHash;
        this.ledgerInflightReservation = ledgerInflightReservation;
        this.ledgerInflightCoalescer = ledgerInflightCoalescer;
        this.ledgerBalanceClient = ledgerBalanceClient;
        this.ledgerInflightOutbox = ledgerInflightOutbox;
        this.meterRegistry = meterRegistry;
        this.orderControlAdmission = orderControlAdmission;
        this.clusterIngressClient = clusterIngressClient;
    }

    /**
     * Result of an idempotent ingress call. {@link #created} tells the controller whether the
     * cluster admitted this submission as a fresh order ({@code true} — respond 201) or as an
     * idempotent re-hit on a prior {@code (account_id, client_idempotency_key)} ({@code false} —
     * respond 200). The {@link #order} carries the cluster's authoritative {@code orderId} —
     * even on duplicates, the response echoes the original orderId so callers see a single
     * identity.
     *
     * <p>Phase 2 slice 2c (oms-aeron-cluster-substrate plan): the orders row may not yet be
     * visible in Postgres at this point. Tests that need the row materialised wait for the
     * projector via Awaitility; production callers should treat the response as
     * cluster-committed and read-after-write only after a small delay (or via the cluster's
     * read APIs once they exist).
     */
    public record IngressResult(Order order, boolean created) {}

    /**
     * Drives the cluster admission and writes the Phase 2-slice-2c residual outbox / fanout rows.
     *
     * <p>The {@code orders} INSERT moved out of this method in slice 2c — the
     * {@code oms-postgres-projector} JVM owns it now, applying the cluster-emitted
     * {@code OrderAdmittedEvent} idempotently. Returning from this method means "cluster
     * committed"; the Postgres orders row materialises shortly after via the projector.
     *
     * <p>Phase 4 Tier 2.5 phase C-2: the Ledger {@code GET /balances/{id}} sync HTTP call
     * for {@link #maybeVerifyLedgerBalanceBinding} runs <strong>before</strong> any tx opens.
     * Phase 4 Tier 2.5 phase D-1: {@link #submitToClusterOrThrow} also runs
     * <strong>before</strong> the tx opens. Pop! 2026-05-14 C-4 sweep showed Hikari pool size
     * was not the lever — at saturation, the conn-holders were threads parked on
     * {@code OmsClusterIngressClient.submitAcceptOrder → CompletableFuture.get} (waiting on
     * the Aeron cluster commit reply) <em>inside</em> {@code transactionTemplate.execute}.
     * The Aeron cluster commit RTT (mean ~0.9 ms, p999 ~11 ms) was being charged against
     * the Hikari connection-hold budget.
     *
     * <p>Phase 4 Tier 2.5 phase D-3 retired the Spring tx entirely: the projector emits the
     * {@code OrderAccepted} envelope from {@link com.balh.oms.cluster.OrderAdmittedEvent}
     * (gated on {@link OrdersRepository#insertFromAdmittedEvent} returning a fresh insert),
     * so the only Postgres write that may still happen here is the optional BUY-async
     * {@code ledger_inflight_outbox} INSERT inside {@link #maybePlaceBuyLedgerInflightHold}.
     * That single statement auto-commits via Hikari's default {@code autoCommit=true};
     * a {@code TransactionTemplate} would only widen the conn-hold window without buying
     * any consistency.
     *
     * <p><strong>Durability gap closure (D-1 + D-3).</strong> The cluster log is the source
     * of truth for "this order was admitted"; it commits independently of any Postgres
     * write here. If the ingress JVM crashes after {@link #submitToClusterOrThrow} returns,
     * the projector idempotently backfills both
     * <ul>
     *   <li>the {@code ledger_inflight_outbox} row (D-1, via
     *       {@code uq_ledger_inflight_outbox_order_id} ON CONFLICT DO NOTHING), and</li>
     *   <li>the {@code OrderAccepted} envelope (D-3, gated on the projector's own
     *       {@code orders} ON CONFLICT DO NOTHING returning a fresh insert).</li>
     * </ul>
     * The whole "ingress crashed mid-write" state therefore no longer leaves missing rows
     * downstream; the projector reconstructs both from the cluster's authoritative event.
     *
     * <p>Behaviour-preserving: error semantics for verify + cluster admit are unchanged
     * ({@code ledger_identity_required} / {@code ledger_identity_mismatch} /
     * {@code ledger_balance_not_found} / {@code ledger_identity_lookup_failed} /
     * {@code cluster_admission_timeout} / {@code cluster_unavailable} /
     * {@code cluster_rejected} all surface as the same HTTP status codes) and the
     * {@code oms.pipeline.ingress.accept_seconds} timer still wraps the full method
     * surface from a caller's perspective.
     */
    public IngressResult persistAccepted(CreateOrderRequest req) {
        Timer.Sample ingressSample = Timer.start(meterRegistry);
        try {
            // Pre-cluster phase: HTTP / cluster work that must NOT hold a Hikari connection.
            //   1. Ledger balance/identity verify (HTTP; pulled out of tx in C-2).
            //   2. Aeron cluster admit (CompletableFuture wait; pulled out of tx in D-1).
            // Both can fail and short-circuit before any Postgres conn is acquired.
            maybeVerifyLedgerBalanceBinding(req);

            UUID id = UUID.randomUUID();
            Instant now = Instant.now();
            int shardId = ShardKey.shardFor(req.accountId(), config.getShard().getCount());
            String accountIdHash = piiHash.hash(req.accountId());
            String ledgerBalanceId = normalizeLedgerBalanceId(req.ledgerBalanceId());
            Order order = buildOrder(id, req, shardId, accountIdHash, ledgerBalanceId, now);

            AdmissionResult ar = submitToClusterOrThrow(clusterIngressClient, order, accountIdHash, now);
            AdmissionResult.Accepted accepted = (AdmissionResult.Accepted) ar;
            boolean created = !accepted.event().duplicate();
            if (accepted.event().duplicate() && !accepted.event().orderId().equals(id)) {
                // Cluster idempotency: an earlier submission for this (accountId, idempotencyKey) won.
                // Echo the original orderId in the response body so the caller sees a single identity.
                id = accepted.event().orderId();
                order = buildOrder(id, req, shardId, accountIdHash, ledgerBalanceId, now);
            }

            if (!created) {
                // Duplicate at the cluster: the orders row + outbox rows were already produced by
                // the original submission. Return without touching Postgres; the projector does NOT
                // re-emit OrderAdmittedEvent on idempotent re-hits (see slice 2b-1), and re-inserting
                // the side tables would create spurious extra domain envelopes / ledger holds.
                OmsPipelineMetrics.finishIngressAccept(meterRegistry, ingressSample, "duplicate");
                return new IngressResult(order, false);
            }

            // D-3 post-cluster phase: at most one auto-committing JDBC INSERT
            // (ledger_inflight_outbox, BUY-async path only). No Spring tx — Hikari's default
            // autoCommit=true commits per statement, and the projector backfills this row from
            // the cluster event if we crash before this method returns (see Javadoc above).
            maybePlaceBuyLedgerInflightHold(order);

            OmsPipelineMetrics.finishIngressAccept(meterRegistry, ingressSample, "created");
            return new IngressResult(order, true);
        } catch (RuntimeException e) {
            OmsPipelineMetrics.finishIngressAccept(meterRegistry, ingressSample, "error");
            throw e;
        }
    }

    private Order buildOrder(
            UUID id,
            CreateOrderRequest req,
            int shardId,
            String accountIdHash,
            String ledgerBalanceId,
            Instant now) {
        return new Order(
                id,
                req.accountId(),
                req.clientIdempotencyKey(),
                shardId,
                0,
                OrderStatus.NEW,
                null,
                req.side(),
                req.instrumentSymbol(),
                req.quantity(),
                req.limitPrice(),
                req.timeInForce(),
                now,
                now,
                null,
                accountIdHash,
                ledgerBalanceId,
                BigDecimal.ZERO);
    }

    private AdmissionResult submitToClusterOrThrow(
            OmsClusterIngressClient cluster, Order order, String accountIdHash, Instant now) {
        AcceptOrderCommand cmd = buildAcceptOrderCommand(cluster, order, accountIdHash, now);
        Duration timeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        AdmissionResult result;
        try {
            result = cluster.submitAcceptOrder(cmd, timeout);
        } catch (TimeoutException e) {
            throw new ClusterAdmissionException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "cluster_admission_timeout",
                    "OMS cluster did not respond within " + timeout.toMillis() + "ms",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterAdmissionException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "cluster_admission_interrupted",
                    "OMS cluster admission was interrupted",
                    e);
        } catch (IllegalStateException e) {
            throw new ClusterAdmissionException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "cluster_unavailable",
                    "OMS cluster client is not connected",
                    e);
        }
        if (result instanceof AdmissionResult.Rejected r) {
            throw new ClusterAdmissionException(
                    HttpStatus.UNPROCESSABLE_ENTITY,
                    "cluster_rejected",
                    "OMS cluster rejected order: " + r.event().reason());
        }
        return result;
    }

    private AcceptOrderCommand buildAcceptOrderCommand(
            OmsClusterIngressClient cluster, Order order, String accountIdHash, Instant now) {
        long quantityScaled;
        try {
            quantityScaled = order.quantity().movePointRight(9).longValueExact();
        } catch (ArithmeticException e) {
            throw new ClusterAdmissionException(
                    HttpStatus.BAD_REQUEST,
                    "quantity_unrepresentable",
                    "quantity " + order.quantity()
                            + " cannot be represented at AcceptOrderCommand quantity scale (1e9)",
                    e);
        }
        long limitPriceScaled = 0L;
        if (order.limitPrice() != null) {
            try {
                limitPriceScaled = order.limitPrice().movePointRight(6).longValueExact();
            } catch (ArithmeticException e) {
                throw new ClusterAdmissionException(
                        HttpStatus.BAD_REQUEST,
                        "limit_price_unrepresentable",
                        "limitPrice " + order.limitPrice()
                                + " cannot be represented at AcceptOrderCommand price scale (1e6)",
                        e);
            }
        }
        byte sideByte = order.side() == Side.BUY ? AcceptOrderCommand.SIDE_BUY : AcceptOrderCommand.SIDE_SELL;
        byte tifByte = tifByteFromString(order.timeInForce());
        long correlationId = cluster.nextCorrelationId();
        return new AcceptOrderCommand(
                correlationId,
                order.id(),
                Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano(),
                quantityScaled,
                limitPriceScaled,
                order.shardId(),
                sideByte,
                tifByte,
                order.accountId().toString(),
                order.clientIdempotencyKey(),
                accountIdHash,
                order.instrumentSymbol(),
                order.ledgerBalanceId());
    }

    private static byte tifByteFromString(String tif) {
        if (tif == null) {
            throw new ClusterAdmissionException(
                    HttpStatus.BAD_REQUEST, "tif_required", "timeInForce is required for cluster admission");
        }
        return switch (tif.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "DAY" -> AcceptOrderCommand.TIF_DAY;
            case "IOC" -> AcceptOrderCommand.TIF_IOC;
            case "FOK" -> AcceptOrderCommand.TIF_FOK;
            case "GTC" -> AcceptOrderCommand.TIF_GTC;
            default -> throw new ClusterAdmissionException(
                    HttpStatus.BAD_REQUEST,
                    "tif_unsupported",
                    "unsupported timeInForce '" + tif + "'; expected DAY/IOC/FOK/GTC");
        };
    }

    private void maybeVerifyLedgerBalanceBinding(CreateOrderRequest req) {
        String balanceId = req.ledgerBalanceId();
        if (balanceId == null) {
            return;
        }
        String claimedIdentity = req.ledgerIdentityId();
        if (claimedIdentity == null || claimedIdentity.isBlank()) {
            throw new LedgerBindingException(HttpStatus.BAD_REQUEST, "ledger_identity_required",
                    "ledgerIdentityId is required when ledgerBalanceId is set");
        }
        LedgerBalanceClient client = ledgerBalanceClient.getIfAvailable();
        if (client == null) {
            throw new LedgerBindingException(HttpStatus.BAD_REQUEST, "ledger_verification_unavailable",
                    "oms.ledger.enabled must be true to accept orders with ledgerBalanceId");
        }
        try {
            String actual = client.fetchIdentityIdForBalance(balanceId);
            if (!actual.trim().equalsIgnoreCase(claimedIdentity.trim())) {
                throw new LedgerBindingException(HttpStatus.NOT_FOUND, "ledger_identity_mismatch",
                        "ledgerIdentityId does not match the balance owner in Ledger");
            }
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            if ("ledger balance not found".equals(e.getMessage())) {
                throw new LedgerBindingException(HttpStatus.NOT_FOUND, "ledger_balance_not_found",
                        "Ledger has no such balance", e);
            }
            String msg = e.getMessage() != null ? e.getMessage() : "ledger error";
            throw new LedgerBindingException(HttpStatus.BAD_GATEWAY, "ledger_identity_lookup_failed", msg, e);
        }
    }

    private void maybePlaceBuyLedgerInflightHold(Order order) {
        if (!config.getLedger().isInflightReservationEnabled()) {
            return;
        }
        if (order.side() != Side.BUY) {
            return;
        }
        if (order.ledgerBalanceId() == null || order.ledgerBalanceId().isBlank()) {
            return;
        }
        if (order.limitPrice() == null) {
            return;
        }
        // Slice 4q: coalescer takes priority when enabled — both async-outbox (4p) and
        // sync-HTTP paths remain available so operators can flip back. Coalescer reuses the
        // outbox as its fallback path on flush failure, so {@code inflightCompensatorEnabled}
        // still backstops correctness end-to-end.
        if (config.getLedger().isInflightCoalescerEnabled()) {
            LedgerInflightCoalescer coalescer = ledgerInflightCoalescer.getIfAvailable();
            if (coalescer != null) {
                placeBuyLedgerInflightHoldThroughCoalescer(order, coalescer);
                return;
            }
            // Coalescer flag on but bean missing (mis-wiring): fall through to async/sync paths
            // rather than silently dropping the hold.
            log.warn("inflight-coalescer-enabled=true but no LedgerInflightCoalescer bean; "
                    + "falling back to async/sync path for orderId={}", order.id());
        }
        if (config.getLedger().isInflightAsyncEnabled()) {
            enqueueBuyLedgerInflightHold(order);
            return;
        }
        LedgerInflightReservationClient client = ledgerInflightReservation.getIfAvailable();
        if (client == null) {
            return;
        }
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            client.placeBuyNotionalHold(order.id(), order.ledgerBalanceId(), order.quantity(), order.limitPrice());
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger sync inflight hold HTTP call at order accept")
                    .tag("result", "success")
                    .tag("path", "sync")
                    .register(meterRegistry));
        } catch (LedgerInflightReservationClient.LedgerReservationException e) {
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger sync inflight hold HTTP call at order accept")
                    .tag("result", "failure")
                    .tag("path", "sync")
                    .register(meterRegistry));
            throw new RuntimeException("ledger inflight reservation failed", e);
        }
    }

    private void placeBuyLedgerInflightHoldThroughCoalescer(Order order, LedgerInflightCoalescer coalescer) {
        Timer.Sample sample = Timer.start(meterRegistry);
        long timeoutMs = config.getLedger().getInflightCoalescerSubmitTimeoutMs();
        CompletableFuture<Void> ack;
        try {
            ack = coalescer.submit(order.id(), order.ledgerBalanceId(), order.quantity(), order.limitPrice());
        } catch (IllegalStateException e) {
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger inflight coalescer hold at order accept")
                    .tag("result", "failure")
                    .tag("path", "coalescer")
                    .register(meterRegistry));
            throw new RuntimeException("ledger inflight coalescer rejected submit: " + e.getMessage(), e);
        }
        try {
            ack.get(timeoutMs, TimeUnit.MILLISECONDS);
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger inflight coalescer hold at order accept")
                    .tag("result", "success")
                    .tag("path", "coalescer")
                    .register(meterRegistry));
        } catch (TimeoutException e) {
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger inflight coalescer hold at order accept")
                    .tag("result", "timeout")
                    .tag("path", "coalescer")
                    .register(meterRegistry));
            // The future is still pending — coalescer may still flush + outbox-fallback after we
            // throw. The compensator path handles any orphaned admit since we roll back the
            // ingress accept tx (no domain event emitted) and the cluster orderId stays admitted.
            throw new RuntimeException(
                    "ledger inflight coalescer ack timed out after " + timeoutMs + "ms", e);
        } catch (ExecutionException e) {
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger inflight coalescer hold at order accept")
                    .tag("result", "failure")
                    .tag("path", "coalescer")
                    .register(meterRegistry));
            throw new RuntimeException("ledger inflight coalescer ack failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger inflight coalescer hold at order accept")
                    .tag("result", "interrupted")
                    .tag("path", "coalescer")
                    .register(meterRegistry));
            throw new RuntimeException("ledger inflight coalescer ack interrupted", e);
        }
    }

    private void enqueueBuyLedgerInflightHold(Order order) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("ledgerBalanceId", order.ledgerBalanceId());
            node.put("quantity", order.quantity().toPlainString());
            node.put("limitPrice", order.limitPrice().toPlainString());
            ledgerInflightOutbox.insert(order.id(), objectMapper.writeValueAsString(node));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise ledger inflight outbox payload for orderId={}", order.id(), e);
            throw new RuntimeException("ledger inflight outbox payload serialisation failed", e);
        }
    }

    private static String normalizeLedgerBalanceId(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        return t.isEmpty() ? null : t;
    }
}
