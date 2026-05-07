package com.balh.oms.fix;

import com.balh.oms.routing.RouteDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FIX outbound route (slice 4 start): implements {@link RouteDispatcher} by enqueueing
 * {@code WORKING} order ids until QuickFIX/J {@code Initiator} send is wired.
 *
 * <p>Next steps: drain this queue from a session callback after logon, map {@link com.balh.oms.domain.Order}
 * to {@code NewOrderSingle}, and route inbound {@code ExecutionReport} to {@link com.balh.oms.returnpath.ExecutionReportApplier}.
 */
public final class FixRouteDispatcher implements RouteDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FixRouteDispatcher.class);

    /** Bounded queue size — tune via env when outbound worker lands. */
    private static final int DEFAULT_FIX_OUTBOUND_QUEUE_CAPACITY = 10_000;

    private final BlockingQueue<UUID> pendingOrderIds = new LinkedBlockingQueue<>(DEFAULT_FIX_OUTBOUND_QUEUE_CAPACITY);

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        if (!pendingOrderIds.offer(orderId)) {
            log.error("FIX outbound pending queue full; dropping orderId={}", orderId);
            return;
        }
        log.info("FIX outbound queued orderId={} (Initiator send path pending slice 4)", orderId);
    }

    public int pendingCountForTests() {
        return pendingOrderIds.size();
    }
}
