package com.balh.oms.chronicle;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides a no-op journal when no {@link ChronicleControlJournal} is in the context
 * (tests with {@code test} profile, or {@code oms.chronicle.enabled=false} so
 * {@link com.balh.oms.config.ChronicleQueueConfiguration} is not loaded).
 *
 * <p>Use {@code ChronicleControlJournal} (not {@code ChronicleQueue}) for
 * {@link ConditionalOnMissingBean}: in some startup orders Spring can register the queue
 * bean after this condition is evaluated, which incorrectly left both {@code NoOpControlJournal}
 * and {@code ChronicleControlJournal} active and broke {@code OutboxReconciler} injection.
 */
@Configuration
public class ControlJournalFallbackConfiguration {

    @Bean
    @ConditionalOnMissingBean(ChronicleControlJournal.class)
    public NoOpControlJournal noopControlJournal() {
        return new NoOpControlJournal();
    }
}
