package com.balh.oms.ingress;

import com.balh.oms.domain.Order;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.settlement.OrderAggregateSettlementStatus;
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
 *       transaction (orders insert + {@code control_outbox} insert +
 *       {@code domain_event_outbox} insert + COMMIT).</li>
 *   <li>{@link com.balh.oms.reconciler.DomainFanoutReconciler} delivers domain
 *       envelopes to NATS (or no-op) strictly after commit.</li>
 *   <li>The Chronicle append happens asynchronously in
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
    private final ExecutionsRepository executions;
    private final Counter ordersCreatedCounter;
    private final Counter ordersDuplicateCounter;

    public OrdersController(
            OrderIngressService ingress,
            OrdersRepository orders,
            ExecutionsRepository executions,
            MeterRegistry registry) {
        this.ingress = ingress;
        this.orders = orders;
        this.executions = executions;
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

        Order accepted = result.order();
        ordersCreatedCounter.increment();

        return ResponseEntity.status(HttpStatus.CREATED).body(CreateOrderResponse.from(accepted));
    }

    @GetMapping("/{id}")
    public ResponseEntity<CreateOrderResponse> getOrder(@PathVariable UUID id) {
        return orders.findById(id)
                .map(o -> {
                    var tradeStatuses = executions.listTradeSettlementStatusesForOrder(o.id());
                    String aggregate = OrderAggregateSettlementStatus.summarize(tradeStatuses);
                    return ResponseEntity.ok(CreateOrderResponse.from(o, aggregate));
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
