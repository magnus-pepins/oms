package com.balh.oms.ingress;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.ShardKey;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.persistence.ControlOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Owns the single Postgres transaction that inserts an order and its matching
 * outbox row.
 *
 * <p>This is a separate Spring bean (not a controller method) because Spring's
 * {@code @Transactional} relies on AOP proxies and self-invocation through
 * {@code this} would skip the proxy. Keeping this in its own bean is what
 * actually opens the transaction at the right boundary.
 */
@Service
public class OrderIngressService {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressService.class);

    private final OrdersRepository orders;
    private final ControlOutboxRepository outbox;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final PiiHash piiHash;

    public OrderIngressService(
            OrdersRepository orders,
            ControlOutboxRepository outbox,
            OmsConfig config,
            ObjectMapper objectMapper,
            PiiHash piiHash) {
        this.orders = orders;
        this.outbox = outbox;
        this.config = config;
        this.objectMapper = objectMapper;
        this.piiHash = piiHash;
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
     * <p>Commit happens when this method returns.
     */
    @Transactional
    public IngressResult persistAccepted(CreateOrderRequest req) {
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
                ledgerBalanceId
        );

        try {
            orders.insert(order);
        } catch (OrdersRepository.DuplicateOrderException e) {
            Order existing = orders.findByIdempotency(req.accountId(), req.clientIdempotencyKey())
                    .orElseThrow(() -> new IllegalStateException(
                            "duplicate detected but no row visible: " + e.getMessage()));
            return new IngressResult(existing, false);
        }

        outbox.insert(id, order.version(),
                serializePayload(new PendingControlEvent(
                        "OrderAccepted",
                        id,
                        order.version(),
                        order.shardId(),
                        order.accountIdHash(),
                        order.acceptedAt(),
                        Instant.now())));
        return new IngressResult(order, true);
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
