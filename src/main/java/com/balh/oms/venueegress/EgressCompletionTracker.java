package com.balh.oms.venueegress;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 * <p>Cursor-only fragments ({@link #registerCursorOnly(long)}, e.g. {@code ExecutionApplied}
 * events the egress does not act on) are queued in log order and flushed once every in-flight admit
 * <em>before</em> them has completed. Unlike the pre-2026-06-05 quiesce path, registering a
 * cursor-only checkpoint does not block the replay thread on unrelated later admits.
 *
 * <h2>Threading</h2>
 *
 * {@link #register(long)}, {@link #registerCursorOnly(long)}, and {@link #pollContiguous()} are
 * called only by the replay / cursor-drain threads and serialise on {@link #replayLock}.
 * {@link #complete(long)} is lock-free (concurrent set membership only) so ER-offer workers at
 * 150+ admits/s do not queue behind {@link #pollContiguous()} or each other.
 *
 * <p>By construction {@link #register} for a position always precedes {@link #complete} for the
 * same position (a fragment is registered before it is written to the venue stream, and completed
 * only when its ack returns). A {@code complete} for an unregistered position is therefore a
 * programming bug; it is ignored here rather than allowed to leak into the completed set.
 */
final class EgressCompletionTracker {

    /** Serialises replay-thread-only deque mutation ({@link #register}, {@link #pollContiguous}). */
    private final Object replayLock = new Object();

    /** Registered-but-not-yet-flushed admit positions, FIFO in strictly-increasing order. */
    private final Deque<Long> pending = new ArrayDeque<>();

    /**
     * Membership view of {@link #pending} for O(1) lock-free {@link #complete(long)} under burst
     * load. {@link #pollContiguous()} removes entries when the contiguous prefix is flushed.
     */
    private final Set<Long> pendingMembership = ConcurrentHashMap.newKeySet();

    /** Cursor-only checkpoints (no venue route) waiting for prior admits to flush. */
    private final Deque<Long> cursorOnlyPending = new ArrayDeque<>();

    /**
     * Completed admit positions still waiting for their predecessors before they can be flushed.
     * Concurrent set so {@link #complete(long)} never acquires {@link #replayLock}.
     */
    private final Set<Long> completed = ConcurrentHashMap.newKeySet();

    private long lastRegistered = Long.MIN_VALUE;

    void register(long position) {
        synchronized (replayLock) {
            requireIncreasing(position);
            pending.addLast(position);
            pendingMembership.add(position);
        }
    }

    /**
     * Records a cursor-only fragment (e.g. {@code ExecutionApplied}) in log order. The replay thread
     * may continue dispatching later admits without blocking here.
     */
    void registerCursorOnly(long position) {
        synchronized (replayLock) {
            requireIncreasing(position);
            cursorOnlyPending.addLast(position);
        }
    }

    /**
     * Marks a registered position complete. Lock-free: safe to call from many ER-offer workers
     * without serialising on {@link #pollContiguous()}.
     */
    void complete(long position) {
        if (pendingMembership.remove(position)) {
            completed.add(position);
        }
    }

    /**
     * Pops every leading position whose ack has arrived (and any cursor-only checkpoints that are
     * unblocked) and returns the highest one popped, or empty if the head of the queue is still in
     * flight. The returned value is the position the caller should advance the persisted cursor to.
     */
    OptionalLong pollContiguous() {
        synchronized (replayLock) {
            long highest = Long.MIN_VALUE;
            boolean advanced = false;
            while (true) {
                boolean progress = false;
                Long head = pending.peekFirst();
                if (head != null && completed.remove(head)) {
                    pending.pollFirst();
                    pendingMembership.remove(head);
                    highest = head;
                    advanced = true;
                    progress = true;
                }
                while (!cursorOnlyPending.isEmpty()) {
                    long checkpoint = cursorOnlyPending.peekFirst();
                    Long nextAdmit = pending.peekFirst();
                    if (nextAdmit != null && nextAdmit < checkpoint) {
                        break;
                    }
                    cursorOnlyPending.pollFirst();
                    highest = checkpoint;
                    advanced = true;
                    progress = true;
                }
                if (!progress) {
                    break;
                }
            }
            return advanced ? OptionalLong.of(highest) : OptionalLong.empty();
        }
    }

    /** Registered admits not yet flushed via {@link #pollContiguous()} (in flight or complete-but-blocked). */
    int inFlight() {
        synchronized (replayLock) {
            return pending.size();
        }
    }

    /**
     * {@code true} when nothing is registered-but-unflushed — used as the quiesce gate at
     * boundaries/shutdown.
     */
    boolean isDrained() {
        synchronized (replayLock) {
            return pending.isEmpty() && cursorOnlyPending.isEmpty();
        }
    }

    private void requireIncreasing(long position) {
        if (position <= lastRegistered) {
            throw new IllegalArgumentException(
                    "egress positions must be registered in strictly-increasing order; got "
                            + position + " after " + lastRegistered);
        }
        lastRegistered = position;
    }
}
