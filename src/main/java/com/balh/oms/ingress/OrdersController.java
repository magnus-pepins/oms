package com.balh.oms.ingress;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.settlement.OrderAggregateSettlementStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * Internal HTTP surface for slice 1.
 *
 * <p>The {@link #createOrder} flow enforces the slice-1 invariant:
 * <ol>
 *   <li>{@link OrderIngressService#persistAccepted} runs the mandatory cluster admission and
 *       (only then) the single Postgres transaction ({@code domain_event_outbox} insert +
 *       optional {@code ledger_inflight_outbox} insert + COMMIT). The {@code orders} INSERT
 *       moved to {@link com.balh.oms.projector.OmsPostgresProjector} in slice 2c, and
 *       {@code control_outbox} was deleted in slice 3f of the Aeron Cluster substrate plan.</li>
 *   <li>{@link com.balh.oms.reconciler.DomainFanoutReconciler} delivers domain
 *       envelopes to NATS (or no-op) strictly after commit.
 *       This controller is not registered on Spring profiles
 *       {@value com.balh.oms.config.OmsProfiles#POSTGRES_PROJECTOR} or
 *       {@value com.balh.oms.config.OmsProfiles#FIX_EGRESS}.</li>
 * </ol>
 *
 * <p>Idempotent re-submissions return {@code 200 OK} with the existing order;
 * uniqueness is guaranteed by
 * {@code UNIQUE (account_id, client_idempotency_key)} on the {@code orders}
 * table.
 */
@RestController
@RequestMapping("/internal/v1/orders")
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
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
                    var expectedDate = executions.maxExpectedSettlementDateForOpenTradeLegs(o.id()).orElse(null);
                    return ResponseEntity.ok(CreateOrderResponse.from(o, aggregate, expectedDate));
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Wed-demo (d2_fe_swap_oms): newest-first list of an account's orders for the customer-FE
     * "my orders" surface. Account scoping is enforced at the BFF (per-user auth, account ==
     * authenticated user); this controller only sees an opaque {@code accountId} query param
     * and trusts the BFF's binding. {@code limit} defaults to 50 (matches the Alpaca my-orders
     * page size today); upper-bounded at 200 so a misbehaving client can't pull the table.
     *
     * <p>Settlement-status aggregate is computed per-order — same shape as {@link #getOrder}
     * — so the FE can render the chip without an extra round-trip. Skipped only when the
     * order has no trade executions yet (the field is nullable on the DTO).
     */
    @GetMapping
    public ResponseEntity<List<CreateOrderResponse>> listOrdersByAccount(
            @RequestParam("accountId") UUID accountId,
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        int clamped = Math.min(Math.max(limit, 1), 200);
        List<Order> rows = orders.findByAccount(accountId, clamped);
        List<CreateOrderResponse> dtos = rows.stream()
                .map(o -> {
                    var tradeStatuses = executions.listTradeSettlementStatusesForOrder(o.id());
                    String aggregate = OrderAggregateSettlementStatus.summarize(tradeStatuses);
                    var expectedDate = executions.maxExpectedSettlementDateForOpenTradeLegs(o.id()).orElse(null);
                    return CreateOrderResponse.from(o, aggregate, expectedDate);
                })
                .toList();
        return ResponseEntity.ok(dtos);
    }
}
