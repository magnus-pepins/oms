package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.ShardKey;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightCoalescer;
import com.balh.oms.ledger.LedgerInflightReservationClient;
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
 * idempotency for {@code OrderWorking}).
 *
 * <p>Phase 4 Tier 2.5 phase D-8 cached the synchronous Ledger {@code GET /balances/{id}}
 * verify in {@link com.balh.oms.ledger.CachingLedgerBalanceClient} (Caffeine, JVM-local).
 *
 * <p>Phase 4 Tier 2.5 phase D-9 retires the last residual ingress-side Postgres write: the
 * BUY-async {@code ledger_inflight_outbox} INSERT moves to the projector. The projector's
 * idempotent {@code insertIfAbsent} (D-1) was already wired as a crash-window backfill;
 * D-9 promotes it to <strong>the only writer</strong> on the BUY-async path. Pop! D-8 jstack
 * at c1600 / 20 800 rps showed 186/200 ingress exec threads parked in
 * {@code LedgerInflightOutboxRepository.insert → ConcurrentBag.borrow} (Hikari pool
 * starvation), so removing the write is the largest remaining single lever — the projector
 * already did the same insert idempotently a few ms later, this just lets the customer-facing
 * 201 not wait on a Hikari connection at all.
 *
 * <p>Net effect after D-9: <strong>every</strong> path through this method opens
 * <strong>zero</strong> Postgres connections on the hot path. Synchronous-hold and
 * coalescer paths still call Ledger / the coalescer respectively (no Postgres there);
 * BUY-async simply returns once the cluster has admitted, and the projector handles the
 * outbox row. SELL / BUY-without-balance / BUY-without-limit-price already short-circuited
 * before any I/O.
 */
@Service
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderIngressService {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressService.class);

    /** Micrometer name for Ledger sync inflight hold latency (tag {@code result}). */
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private final OrdersRepository orders;
    private final OmsConfig config;
    private final PiiHash piiHash;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;
    private final MeterRegistry meterRegistry;
    private final OrderControlAdmission orderControlAdmission;
    /**
     * Phase 4 Tier 2.5 phase E-1 — admit submission goes through the shard router so this
     * service does not need to know which {@link OmsClusterIngressClient} owns the target shard.
     * At {@code shardCount=1} the router resolves to the single client (byte-identical to the
     * pre-E-1 direct injection); at higher shard counts (E-3+) the router fans out to N clients
     * keyed by {@code accountId} hash without changing this call site.
     */
    private final OmsClusterShardRouter clusterShardRouter;

    public OrderIngressService(
            OrdersRepository orders,
            OmsConfig config,
            PiiHash piiHash,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            MeterRegistry meterRegistry,
            OrderControlAdmission orderControlAdmission,
            OmsClusterShardRouter clusterShardRouter) {
        this.orders = orders;
        this.config = config;
        this.piiHash = piiHash;
        this.ledgerInflightReservation = ledgerInflightReservation;
        this.ledgerInflightCoalescer = ledgerInflightCoalescer;
        this.ledgerBalanceClient = ledgerBalanceClient;
        this.meterRegistry = meterRegistry;
        this.orderControlAdmission = orderControlAdmission;
        this.clusterShardRouter = clusterShardRouter;
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
     * (gated on {@link OrdersRepository#insertFromAdmittedEvent} returning a fresh insert).
     *
     * <p>Phase 4 Tier 2.5 phase D-9 retires the BUY-async {@code ledger_inflight_outbox}
     * INSERT too: the projector's {@code insertIfAbsent} (originally D-1's crash-window
     * safety net) is now the only writer on the BUY-async path. Ingress no longer touches
     * Postgres at all on this branch.
     *
     * <p><strong>Durability via cluster log (D-1 + D-3 + D-9).</strong> The cluster log is
     * the source of truth for "this order was admitted"; it commits independently of any
     * Postgres write. After D-9, every Postgres row downstream of the cluster log
     * (orders, OrderAccepted envelope, ledger_inflight_outbox) is materialised by the
     * projector from the authoritative {@link com.balh.oms.cluster.OrderAdmittedEvent}:
     * <ul>
     *   <li>the {@code orders} row (slice 2c — projector's
     *       {@link OrdersRepository#insertFromAdmittedEvent} ON CONFLICT DO NOTHING)</li>
     *   <li>the {@code OrderAccepted} envelope in {@code domain_event_outbox} (D-3, gated
     *       on the {@code orders} insert returning fresh)</li>
     *   <li>the {@code ledger_inflight_outbox} row for BUY-async (D-9, idempotent via the
     *       {@code uq_ledger_inflight_outbox_order_id} unique index)</li>
     * </ul>
     * The trade-off: the {@code ledger_inflight_outbox} row now appears after the projector
     * applies the admit instead of inside the ingress accept. The slice 4p reconciler
     * already polls on a configured interval — the per-order delay between cluster admit
     * and Ledger hold POST grows by the projector's per-event latency (sub-ms in steady
     * state, see {@code oms.pipeline.cluster_admit_to_projector_seconds}). Customer-facing
     * 201 happens at cluster admit; the actual hold settles a tick later, which is the
     * same eventual-consistency contract slice 4p already established.
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

            // Slice E-1: route the admit to the cluster client owning this shard. At
            // shardCount=1 this is byte-identical to using the singleton client directly; at
            // shardCount>1 (E-3+) the router fans out to N clients without touching this method.
            // Passing the already-computed shardId (rather than re-deriving from accountId) keeps
            // the order's recorded shard and its admitting cluster identical by construction.
            OmsClusterIngressClient cluster = clusterShardRouter.forShard(shardId);
            AdmissionResult ar = submitToClusterOrThrow(cluster, order, accountIdHash, now);
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

            // D-9 post-cluster phase: only sync-HTTP and coalescer paths still have work
            // here; BUY-async (the production default) returns immediately because the
            // projector's recordLedgerInflightOutboxIfNeeded materialises the outbox row
            // from the cluster's authoritative OrderAdmittedEvent. No Postgres I/O on the
            // hot path for the BUY-async branch.
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
            // Phase 4 Tier 2.5 phase D-9: ingress no longer writes ledger_inflight_outbox.
            // The projector's recordLedgerInflightOutboxIfNeeded(OrderAdmittedEvent) is the
            // only writer; it idempotently inserts the row (ON CONFLICT DO NOTHING on
            // uq_ledger_inflight_outbox_order_id) from the cluster's authoritative admit
            // event, so the slice 4p reconciler/compensator pipeline picks the row up
            // without any change. See class-level Javadoc for the consistency contract.
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

    private static String normalizeLedgerBalanceId(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        return t.isEmpty() ? null : t;
    }
}
