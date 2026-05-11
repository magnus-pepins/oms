package com.balh.oms.fix;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Dequeue from {@link FixRouteDispatcher}'s in-memory queue.
 */
public final class QueueFixOutboundOrderDequeue implements FixOutboundOrderDequeue {

    private final FixRouteDispatcher dispatcher;

    public QueueFixOutboundOrderDequeue(FixRouteDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public UUID pollOrNull() {
        return dispatcher.pollPendingOrNull();
    }

    @Override
    public UUID poll(long timeout, TimeUnit unit) throws InterruptedException {
        return dispatcher.pollPending(timeout, unit);
    }
}
