package com.balh.oms.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides a no-op journal when no {@link ChronicleQueue} is in the context
 * (tests, or {@code oms.chronicle.enabled=false} so {@link ChronicleQueueConfiguration}
 * is not loaded).
 */
@Configuration
public class ControlJournalFallbackConfiguration {

    @Bean
    @ConditionalOnMissingBean(ChronicleQueue.class)
    public NoOpControlJournal noopControlJournal() {
        return new NoOpControlJournal();
    }
}
