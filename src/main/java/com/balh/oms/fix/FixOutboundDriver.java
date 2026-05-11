package com.balh.oms.fix;

/**
 * How {@link FixOutboundDispatchWorker} is woken to drain {@link FixRouteDispatcher}'s in-memory queue.
 */
public enum FixOutboundDriver {

    /**
     * Spring {@link org.springframework.scheduling.config.ScheduledTaskRegistrar} fixed delay after each tick
     * (one dequeue attempt per tick when gates allow). Default.
     */
    SCHEDULED,

    /**
     * Dedicated non-daemon thread: when logon and route send are enabled, blocks on
     * {@link java.util.concurrent.BlockingQueue#poll(long, java.util.concurrent.TimeUnit)} up to
     * {@code outbound-dedicated-idle-park-nanos}; when not ready, parks for {@code outbound-dedicated-not-ready-park-nanos}.
     * See {@code docs/fix-outbound-driver.md}.
     */
    DEDICATED
}
