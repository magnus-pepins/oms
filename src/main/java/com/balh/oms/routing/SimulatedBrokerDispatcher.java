package com.balh.oms.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * Outbound simulated broker seam (slice 3): implements {@link RouteDispatcher} by
 * enqueueing {@code WORKING} order ids for {@link SimulatedReturnPathProjectionWorker}.
 *
 * <p>This is the class name the master plan calls “SimulatedBrokerDispatcher”; the
 * actual synthetic ER projection runs in {@link SimulatedReturnPathProjectionWorker} +
 * {@link SimulatedExecutionProgram} (same process as FIX slice 4’s inbound adapter).
 */
public final class SimulatedBrokerDispatcher implements RouteDispatcher {

    private static final Logger log = LoggerFactory.getLogger(SimulatedBrokerDispatcher.class);

    private final BlockingQueue<UUID> orderQueue;

    public SimulatedBrokerDispatcher(BlockingQueue<UUID> orderQueue) {
        this.orderQueue = orderQueue;
    }

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        if (!orderQueue.offer(orderId)) {
            log.error("Simulated broker queue full; dropping orderId={}", orderId);
        }
    }
}
