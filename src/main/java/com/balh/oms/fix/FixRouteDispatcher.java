package com.balh.oms.fix;

import com.balh.oms.observability.otel.IngressToFixNosLatencyLimits;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.routing.RouteDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private final IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;

    public FixRouteDispatcher(
            BlockingQueue<UUID> pendingOrderIds, IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        this.pendingOrderIds = pendingOrderIds;
        this.ingressToFixNosLatencyRecorder = ingressToFixNosLatencyRecorder;
    }

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        if (!pendingOrderIds.offer(orderId)) {
            ingressToFixNosLatencyRecorder.discard(orderId, IngressToFixNosLatencyLimits.REASON_QUEUE_FULL);
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

    /**
     * Blocking wait for an id (used by {@link FixOutboundDriver#DEDICATED}). Returns {@code null} on timeout or interrupt.
     */
    public UUID pollPending(long timeout, TimeUnit unit) throws InterruptedException {
        return pendingOrderIds.poll(timeout, unit);
    }

    public int pendingCountForTests() {
        return pendingOrderIds.size();
    }
}
