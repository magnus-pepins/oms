package com.balh.oms.ingress;

import com.balh.oms.domain.Order;
import com.balh.oms.events.DomainEventPublisher;
import com.balh.oms.events.OrderAcceptedEvent;
import com.balh.oms.persistence.OrdersRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Internal HTTP surface for slice 1.
 *
 * <p>The {@link #createOrder} flow enforces the slice-1 invariant:
 * <ol>
 *   <li>{@link OrderIngressService#persistAccepted} runs the single Postgres
 *       transaction (orders insert + control_outbox insert + COMMIT).</li>
 *   <li>Only AFTER that returns do we publish the {@link OrderAcceptedEvent}
 *       to the fanout bus. The Chronicle append happens asynchronously in
 *       {@code OutboxReconciler}, also strictly after commit.</li>
 * </ol>
 *
 * <p>Idempotent re-submissions return {@code 200 OK} with the existing order;
 * uniqueness is guaranteed by
 * {@code UNIQUE (account_id, client_idempotency_key)} on the {@code orders}
 * table.
 */
@RestController
@RequestMapping("/internal/v1/orders")
public class OrdersController {

    private final OrderIngressService ingress;
    private final OrdersRepository orders;
    private final DomainEventPublisher events;
    private final Counter ordersCreatedCounter;
    private final Counter ordersDuplicateCounter;

    public OrdersController(
            OrderIngressService ingress,
            OrdersRepository orders,
            DomainEventPublisher events,
            MeterRegistry registry) {
        this.ingress = ingress;
        this.orders = orders;
        this.events = events;
        this.ordersCreatedCounter = Counter.builder("oms_orders_created_total")
                .description("Orders accepted via the internal HTTP ingress")
                .register(registry);
        this.ordersDuplicateCounter = Counter.builder("oms_orders_duplicate_total")
                .description("Idempotent re-submissions of an existing (account, key)")
                .register(registry);
    }

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest req) {
        OrderIngressService.IngressResult result = ingress.persistAccepted(req);

        if (!result.created()) {
            ordersDuplicateCounter.increment();
            return ResponseEntity.ok(CreateOrderResponse.from(result.order()));
        }

        // Publish the domain event AFTER the @Transactional boundary on
        // OrderIngressService.persistAccepted has closed.
        Order accepted = result.order();
        events.publish(new OrderAcceptedEvent(
                accepted.id(),
                accepted.version(),
                accepted.shardId(),
                accepted.accountIdHash(),
                accepted.side().name(),
                accepted.instrumentSymbol(),
                accepted.quantity(),
                accepted.limitPrice(),
                accepted.timeInForce(),
                accepted.acceptedAt()
        ));
        ordersCreatedCounter.increment();

        return ResponseEntity.status(HttpStatus.CREATED).body(CreateOrderResponse.from(accepted));
    }

    @GetMapping("/{id}")
    public ResponseEntity<CreateOrderResponse> getOrder(@PathVariable UUID id) {
        return orders.findById(id)
                .map(o -> ResponseEntity.ok(CreateOrderResponse.from(o)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
