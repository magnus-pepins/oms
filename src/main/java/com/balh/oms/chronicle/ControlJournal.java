package com.balh.oms.chronicle;

/**
 * Append-only durable journal for control-plane events.
 *
 * <p>Slice 1 implementation is shard-local Chronicle Queue OSS. The journal is
 * an <em>engineering replay</em> facility, not a regulatory system of record:
 * Postgres remains the source of truth for orders. See
 * {@code oms/docs/architecture.md} and {@code oms/docs/decisions.md}.
 *
 * <p>Implementations MUST append synchronously and only after the originating
 * Postgres transaction has committed.
 */
public interface ControlJournal {

    /**
     * Append a serialised payload (typically a {@code PendingControlEvent} JSON
     * blob written by the reconciler).
     *
     * @return the index returned by Chronicle, useful for diagnostics.
     */
    long append(byte[] payload);
}
