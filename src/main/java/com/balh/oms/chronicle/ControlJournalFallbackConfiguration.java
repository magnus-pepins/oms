package com.balh.oms.chronicle;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides {@link NoOpControlJournal} when Chronicle is turned off via
 * {@code oms.chronicle.enabled=false} (including the {@code test} profile in
 * {@code application-test.yaml}).
 *
 * <p>Do not use {@code @ConditionalOnMissingBean(ChronicleControlJournal)} here: Spring can
 * register this bean before {@link com.balh.oms.config.ChronicleQueueConfiguration} runs, so both
 * the no-op and real journal would exist and break {@link com.balh.oms.reconciler.OutboxReconciler}.
 */
@Configuration
public class ControlJournalFallbackConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "oms.chronicle", name = "enabled", havingValue = "false", matchIfMissing = false)
    public NoOpControlJournal noopControlJournal() {
        return new NoOpControlJournal();
    }
}
