package com.balh.oms.venueegress;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Tracks the contiguous-completed prefix of an out-of-order-completing egress pipeline so the
 * persisted cursor only ever advances over fragments that have been <em>fully</em> applied.
 *
 * <p>Design A of {@code system-documentation/plans/oms-venue-egress-pipelining.md}. The replay
 * thread {@link #register(long) registers} each dispatched fragment's cluster-log position in
 * strictly-increasing order (Aeron log positions are monotonic). Asynchronous venue-ack handlers
 * {@link #complete(long) complete} positions in arbitrary order as their {@code RouteOrder} acks
 * arrive. The replay thread periodically calls {@link #pollContiguous()} to learn the highest
 * position {@code P} such that every registered position {@code <= P} is complete — the only
 * position it is safe to persist the cursor to (everything after {@code P} is replayed on crash;
 * the venue dedupes the redelivery, exactly as the existing {@code cursorFlushEvery} window does).
 *
 * <h2>Threading</h2>
 *
 * {@link #register(long)} and {@link #pollContiguous()} are called only by the replay thread;
 * {@link #complete(long)} is called by venue-ack handler threads. All three synchronise on
 * {@code this}; per-fragment contention is negligible. {@link #inFlight()} / {@link #isDrained()}
 * support the quiesce-at-boundary protocol.
 *
 * <p>By construction {@link #register} for a position always precedes {@link #complete} for the
 * same position (a fragment is registered before it is written to the venue stream, and completed
 * only when its ack returns). A {@code complete} for an unregistered position is therefore a
 * programming bug; it is ignored here rather than allowed to leak into the completed set.
 */
final class EgressCompletionTracker {

    /** Registered-but-not-yet-flushed positions, FIFO in strictly-increasing order. */
    private final Deque<Long> pending = new ArrayDeque<>();

    /** Completed positions still waiting for their predecessors before they can be flushed. */
    private final Set<Long> completed = new HashSet<>();

    private long lastRegistered = Long.MIN_VALUE;

    synchronized void register(long position) {
        if (position <= lastRegistered) {
            throw new IllegalArgumentException(
                    "egress positions must be registered in strictly-increasing order; got "
                            + position + " after " + lastRegistered);
        }
        lastRegistered = position;
        pending.addLast(position);
    }

    synchronized void complete(long position) {
        // Ignore completions for positions we never registered (cannot happen by construction).
        if (pending.contains(position)) {
            completed.add(position);
        }
    }

    /**
     * Pops every leading position whose ack has arrived and returns the highest one popped, or
     * empty if the head of the queue is still in flight. The returned value is the position the
     * caller should advance the persisted cursor to.
     */
    synchronized OptionalLong pollContiguous() {
        long highest = Long.MIN_VALUE;
        boolean advanced = false;
        Long head;
        while ((head = pending.peekFirst()) != null && completed.remove(head)) {
            pending.pollFirst();
            highest = head;
            advanced = true;
        }
        return advanced ? OptionalLong.of(highest) : OptionalLong.empty();
    }

    /** Registered fragments not yet flushed via {@link #pollContiguous()} (in flight or complete-but-blocked). */
    synchronized int inFlight() {
        return pending.size();
    }

    /** {@code true} when nothing is registered-but-unflushed — used as the quiesce gate at boundaries/shutdown. */
    synchronized boolean isDrained() {
        return pending.isEmpty();
    }
}
