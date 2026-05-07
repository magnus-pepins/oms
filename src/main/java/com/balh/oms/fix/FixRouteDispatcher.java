package com.balh.oms.fix;

import com.balh.oms.routing.RouteDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * FIX outbound route (slice 4): implements {@link RouteDispatcher} by enqueueing {@code WORKING}
 * order ids; {@link FixOutboundDispatchWorker} drains after logon and sends {@code NewOrderSingle}.
 *
 * <p>Inbound {@code ExecutionReport} is handled in {@link OmsFixApplication} → {@link FixInboundHandler}
 * → {@link com.balh.oms.returnpath.ExecutionReportApplier}.
 */
public final class FixRouteDispatcher implements RouteDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FixRouteDispatcher.class);

    private final BlockingQueue<UUID> pendingOrderIds;

    public FixRouteDispatcher(BlockingQueue<UUID> pendingOrderIds) {
        this.pendingOrderIds = pendingOrderIds;
    }

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        if (!pendingOrderIds.offer(orderId)) {
            log.error("FIX outbound pending queue full; dropping orderId={}", orderId);
            return;
        }
        log.debug("FIX outbound queued orderId={}", orderId);
    }

    /**
     * Non-blocking poll for the outbound worker (one id per scheduler tick).
     */
    public UUID pollPendingOrNull() {
        return pendingOrderIds.poll();
    }

    public int pendingCountForTests() {
        return pendingOrderIds.size();
    }
}
