package com.balh.oms.fix;

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
    public FixOutboundWireJob peekOrNull() {
        return dispatcher.peekPendingOrNull();
    }

    @Override
    public FixOutboundWireJob pollOrNull() {
        return dispatcher.pollPendingOrNull();
    }

    @Override
    public FixOutboundWireJob poll(long timeout, TimeUnit unit) throws InterruptedException {
        return dispatcher.pollPending(timeout, unit);
    }
}
