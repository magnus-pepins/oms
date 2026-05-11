package com.balh.oms.chronicle;

/**
 * How {@link ChronicleControlTailReader} waits for new Chronicle excerpts after draining the current batch.
 */
public enum ChronicleTailDriver {

    /**
     * Fixed-delay polling via Spring scheduling ({@code pollBatch} on an interval). Default; predictable CPU.
     */
    SCHEDULED,

    /**
     * Dedicated non-daemon thread that busy-drains up to {@code tail-batch-max-messages} per inner spin, then
     * {@link java.util.concurrent.locks.LockSupport#parkNanos(long)} when the queue is empty. Minimizes
     * scheduler-induced latency between append and apply; see {@code docs/chronicle-tail-driver.md}.
     */
    DEDICATED
}
