package com.balh.oms.routing;

import java.util.UUID;

/**
 * Outbound route dispatch after an order reaches {@code WORKING} (slice 3 seam for FIX in slice 4).
 */
@FunctionalInterface
public interface RouteDispatcher {

    void enqueueWorkingOrder(UUID orderId);
}
