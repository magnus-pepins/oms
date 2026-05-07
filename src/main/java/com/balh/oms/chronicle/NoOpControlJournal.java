package com.balh.oms.chronicle;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link ControlJournal} used in tests and when Chronicle is turned off.
 *
 * <p>Registered via {@link ControlJournalFallbackConfiguration}. Tests can
 * {@code @Autowired NoOpControlJournal} to assert append counts.
 */
public class NoOpControlJournal implements ControlJournal {

    private final AtomicLong index = new AtomicLong(0);

    @Override
    public long append(byte[] payload) {
        return index.incrementAndGet();
    }

    public long appendCount() {
        return index.get();
    }
}
