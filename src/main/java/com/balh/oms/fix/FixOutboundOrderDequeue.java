package com.balh.oms.fix;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Dequeue side for FIX outbound (memory queue vs Postgres handoff).
 */
public interface FixOutboundOrderDequeue {

    /** Non-blocking poll; {@code null} if none. */
    UUID pollOrNull();

    /**
     * Blocking wait up to {@code timeout} for an id, or {@code null} on timeout.
     * Interrupt status is preserved; throws {@link InterruptedException} when interrupted while waiting.
     */
    UUID poll(long timeout, TimeUnit unit) throws InterruptedException;
}
