package com.balh.oms.ingress;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.ShardKey;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.ControlOutboxRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Owns the single Postgres transaction that inserts an order and matching
 * {@code control_outbox} and {@code domain_event_outbox} rows.
 *
 * <p>This is a separate Spring bean (not a controller method) because Spring's
 * {@code @Transactional} relies on AOP proxies and self-invocation through
 * {@code this} would skip the proxy. Keeping this in its own bean is what
 * actually opens the transaction at the right boundary.
 */
@Service
public class OrderIngressService {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressService.class);

    /** Micrometer name for Ledger sync inflight hold latency (tag {@code result}). */
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private final OrdersRepository orders;
    private final ControlOutboxRepository outbox;
    private final DomainEventOutboxRepository domainEventOutbox;
    private final DomainEventEnvelopeCodec domainEventEnvelopeCodec;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final PiiHash piiHash;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;
    private final LedgerInflightOutboxRepository ledgerInflightOutbox;
    private final MeterRegistry meterRegistry;

    public OrderIngressService(
            OrdersRepository orders,
            ControlOutboxRepository outbox,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec domainEventEnvelopeCodec,
            OmsConfig config,
            ObjectMapper objectMapper,
            PiiHash piiHash,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            LedgerInflightOutboxRepository ledgerInflightOutbox,
            MeterRegistry meterRegistry) {
        this.orders = orders;
        this.outbox = outbox;
        this.domainEventOutbox = domainEventOutbox;
        this.domainEventEnvelopeCodec = domainEventEnvelopeCodec;
        this.config = config;
        this.objectMapper = objectMapper;
        this.piiHash = piiHash;
        this.ledgerInflightReservation = ledgerInflightReservation;
        this.ledgerBalanceClient = ledgerBalanceClient;
        this.ledgerInflightOutbox = ledgerInflightOutbox;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Result of an idempotent ingress call. {@link #created} tells the
     * controller whether the row was inserted by this call ({@code true} —
     * respond 201) or already existed ({@code false} — respond 200).
     */
    public record IngressResult(Order order, boolean created) {}

    /**
     * Inserts the order and the outbox row in a single Postgres transaction,
     * or — if another concurrent request beat us to the unique constraint —
     * returns the pre-existing row.
     *
     * <p>Commit happens when this method returns. Domain fanout delivery is
     * asynchronous via {@link com.balh.oms.reconciler.DomainFanoutReconciler}.
     */
    @Transactional
    public IngressResult persistAccepted(CreateOrderRequest req) {
        maybeVerifyLedgerBalanceBinding(req);

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        int shardId = ShardKey.shardFor(req.accountId(), config.getShard().getCount());
        String accountIdHash = piiHash.hash(req.accountId());
        String ledgerBalanceId = normalizeLedgerBalanceId(req.ledgerBalanceId());

        Order order = new Order(
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
                BigDecimal.ZERO
        );

        try {
            orders.insert(order);
        } catch (OrdersRepository.DuplicateOrderException e) {
            Order existing = orders.findByIdempotency(req.accountId(), req.clientIdempotencyKey())
                    .orElseThrow(() -> new IllegalStateException(
                            "duplicate detected but no row visible: " + e.getMessage()));
            return new IngressResult(existing, false);
        }

        maybePlaceBuyLedgerInflightHold(order);

        outbox.insert(id, order.version(),
                serializePayload(new PendingControlEvent(
                        "OrderAccepted",
                        id,
                        order.version(),
                        order.shardId(),
                        order.accountIdHash(),
                        order.acceptedAt(),
                        Instant.now())));
        try {
            domainEventOutbox.insert(id, domainEventEnvelopeCodec.orderAccepted(order));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise domain fanout envelope for orderId={}", id, e);
            throw new RuntimeException("domain event envelope serialisation failed", e);
        }
        return new IngressResult(order, true);
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

    private String serializePayload(PendingControlEvent ev) {
        try {
            return objectMapper.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise PendingControlEvent for orderId={}", ev.orderId(), e);
            throw new RuntimeException("payload serialisation failed", e);
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
