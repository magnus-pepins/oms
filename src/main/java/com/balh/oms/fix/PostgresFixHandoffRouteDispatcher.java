package com.balh.oms.fix;

import com.balh.oms.persistence.FixOutboundHandoffRepository;
import com.balh.oms.routing.RouteDispatcher;

import java.util.UUID;

/**
 * FIX routing enqueue backed by Postgres {@code fix_outbound_handoff} (durable, idempotent insert).
 */
public final class PostgresFixHandoffRouteDispatcher implements RouteDispatcher {

    private final FixOutboundHandoffRepository repository;

    public PostgresFixHandoffRouteDispatcher(FixOutboundHandoffRepository repository) {
        this.repository = repository;
    }

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        repository.enqueue(orderId);
    }
}
