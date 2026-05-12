package com.balh.oms.fix;

import java.util.concurrent.TimeUnit;

/**
 * Dequeue side for FIX outbound (memory queue vs Postgres handoff).
 */
public interface FixOutboundOrderDequeue {

    /** Non-blocking peek at the head job without removing it; {@code null} if empty. */
    FixOutboundWireJob peekOrNull();

    /** Non-blocking poll; {@code null} if none. */
    FixOutboundWireJob pollOrNull();

    /**
     * Blocking wait up to {@code timeout} for a job, or {@code null} on timeout.
     * Interrupt status is preserved; throws {@link InterruptedException} when interrupted while waiting.
     */
    FixOutboundWireJob poll(long timeout, TimeUnit unit) throws InterruptedException;
}
