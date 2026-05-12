package com.balh.oms.fix;

import com.balh.oms.observability.otel.IngressToFixNosLatencyLimits;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.routing.RouteDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.fix44.OrderMassCancelRequest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.UUID;

/**
 * FIX outbound route (slice 4): implements {@link RouteDispatcher} by enqueueing {@code WORKING}
 * order ids; {@link FixOutboundDispatchWorker} drains after logon and sends {@code NewOrderSingle}.
 *
 * <p>Inbound {@code ExecutionReport} is handled in {@link OmsFixApplication} → {@link FixInboundHandler}
 * → {@link com.balh.oms.returnpath.ExecutionReportApplier}.
 */
public final class FixRouteDispatcher implements RouteDispatcher {

    private static final Logger log = LoggerFactory.getLogger(FixRouteDispatcher.class);

    private final BlockingQueue<FixOutboundWireJob> pendingJobs;
    private final IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;

    public FixRouteDispatcher(
            BlockingQueue<FixOutboundWireJob> pendingJobs, IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        this.pendingJobs = pendingJobs;
        this.ingressToFixNosLatencyRecorder = ingressToFixNosLatencyRecorder;
    }

    @Override
    public void enqueueWorkingOrder(UUID orderId) {
        if (!pendingJobs.offer(new FixOutboundWireJob.NosOrder(orderId))) {
            ingressToFixNosLatencyRecorder.discard(orderId, IngressToFixNosLatencyLimits.REASON_QUEUE_FULL);
            log.error("FIX outbound pending queue full; dropping orderId={}", orderId);
            return;
        }
        log.debug("FIX outbound queued orderId={}", orderId);
    }

    /**
     * Enqueue a mass cancel for the outbound worker; blocks until send completes or {@code waitMs} elapses.
     *
     * @throws IllegalStateException when the queue is full
     * @throws ExecutionException when send fails
     * @throws TimeoutException      when not processed within {@code waitMs}
     */
    public void enqueueMassCancelAndAwait(OrderMassCancelRequest message, long waitMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (!pendingJobs.offer(new FixOutboundWireJob.MassCancelWire(message, done))) {
            throw new IllegalStateException("fix_outbound_queue_full");
        }
        done.get(waitMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Non-blocking peek at the head job (used when route send is disabled to unblock {@code MassCancelWire} waiters).
     */
    public FixOutboundWireJob peekPendingOrNull() {
        return pendingJobs.peek();
    }

    /**
     * Non-blocking poll for the outbound worker (one job per scheduler tick).
     */
    public FixOutboundWireJob pollPendingOrNull() {
        return pendingJobs.poll();
    }

    /**
     * Blocking wait for a job (used by {@link FixOutboundDriver#DEDICATED}). Returns {@code null} on timeout or interrupt.
     */
    public FixOutboundWireJob pollPending(long timeout, TimeUnit unit) throws InterruptedException {
        return pendingJobs.poll(timeout, unit);
    }

    public int pendingCountForTests() {
        return pendingJobs.size();
    }
}
