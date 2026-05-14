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
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.DomainEventOutboxRepository;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
 * The {@code domain_event_outbox} insert (for {@code OrderAccepted}) and the optional BUY
 * {@code ledger_inflight_outbox} insert are still done here.
 *
 * <p>This is a separate Spring bean (not a controller method) because Spring's
 * {@code @Transactional} relies on AOP proxies and self-invocation through
 * {@code this} would skip the proxy. Keeping this in its own bean is what actually opens the
 * transaction at the right boundary.
 */
@Service
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderIngressService {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressService.class);

    /** Micrometer name for Ledger sync inflight hold latency (tag {@code result}). */
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private final OrdersRepository orders;
    private final DomainEventOutboxRepository domainEventOutbox;
    private final DomainEventEnvelopeCodec domainEventEnvelopeCodec;
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
    private final TransactionTemplate transactionTemplate;

    public OrderIngressService(
            OrdersRepository orders,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec domainEventEnvelopeCodec,
            OmsConfig config,
            ObjectMapper objectMapper,
            PiiHash piiHash,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            LedgerInflightOutboxRepository ledgerInflightOutbox,
            MeterRegistry meterRegistry,
            OrderControlAdmission orderControlAdmission,
            OmsClusterIngressClient clusterIngressClient,
            PlatformTransactionManager transactionManager) {
        this.orders = orders;
        this.domainEventOutbox = domainEventOutbox;
        this.domainEventEnvelopeCodec = domainEventEnvelopeCodec;
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
        this.transactionTemplate = new TransactionTemplate(transactionManager);
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
     * for {@link #maybeVerifyLedgerBalanceBinding} now runs <strong>before</strong> the
     * Postgres transaction opens. Pop! 2026-05-14 jstacks (post-V30+V31 indexes) showed
     * 17 of ingress-2's 39 http-nio threads RUNNABLE inside
     * {@code HttpClient.parseHTTPHeader} on this verify call, while still holding a Hikari
     * connection acquired by the surrounding {@code @Transactional}. Pulling the verify
     * out of the tx and using an explicit {@link TransactionTemplate} for the
     * Postgres+cluster-commit work makes the conn-hold cover only Postgres time
     * (microseconds) instead of Postgres+Ledger-RTT (10–20 ms).
     *
     * <p>Behaviour-preserving: error semantics for the verify path are unchanged
     * ({@code ledger_identity_required} / {@code ledger_identity_mismatch} /
     * {@code ledger_balance_not_found} / {@code ledger_identity_lookup_failed} still
     * surface as the same HTTP status codes) and the
     * {@code oms.pipeline.ingress.accept_seconds} timer still wraps the full method
     * surface from a caller's perspective.
     */
    public IngressResult persistAccepted(CreateOrderRequest req) {
        Timer.Sample ingressSample = Timer.start(meterRegistry);
        try {
            maybeVerifyLedgerBalanceBinding(req);
            IngressResult result = transactionTemplate.execute(status -> persistAcceptedBody(req));
            if (result == null) {
                throw new IllegalStateException("transactionTemplate returned null IngressResult");
            }
            OmsPipelineMetrics.finishIngressAccept(
                    meterRegistry, ingressSample, result.created() ? "created" : "duplicate");
            return result;
        } catch (RuntimeException e) {
            OmsPipelineMetrics.finishIngressAccept(meterRegistry, ingressSample, "error");
            throw e;
        }
    }

    /**
     * The Postgres-transactional portion of {@link #persistAccepted}. Caller must have
     * already run {@link #maybeVerifyLedgerBalanceBinding} (the only step that did Ledger
     * sync HTTP — see Phase 4 Tier 2.5 phase C-2 note on {@link #persistAccepted}).
     */
    private IngressResult persistAcceptedBody(CreateOrderRequest req) {
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
            // Duplicate at the cluster: the orders row + domain fanout were already produced by
            // the original submission. Return without re-writing them; the projector does NOT
            // re-emit OrderAdmittedEvent on idempotent re-hits (see slice 2b-1), and re-inserting
            // the side-tables would create spurious extra domain envelopes.
            return new IngressResult(order, false);
        }

        maybePlaceBuyLedgerInflightHold(order);

        try {
            domainEventOutbox.insert(id, domainEventEnvelopeCodec.orderAccepted(order));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise domain fanout envelope for orderId={}", id, e);
            throw new RuntimeException("domain event envelope serialisation failed", e);
        }
        return new IngressResult(order, true);
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
