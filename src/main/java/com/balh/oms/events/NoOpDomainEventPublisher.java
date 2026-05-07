package com.balh.oms.events;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * Default in-process publisher. Increments a counter so we can assert that
 * the post-commit hook fires, but does not deliver anywhere.
 *
 * <p>Replace with the NATS publisher when slice 1.5 lands; that PR adds a
 * {@code @ConditionalOnProperty("oms.events.nats.enabled")} bean which
 * displaces this one via {@link ConditionalOnMissingBean} (bean name
 * {@code natsDomainEventPublisher}).
 */
@Component
@ConditionalOnMissingBean(name = "natsDomainEventPublisher")
public class NoOpDomainEventPublisher implements DomainEventPublisher {

    private final Counter publishedCounter;

    public NoOpDomainEventPublisher(MeterRegistry registry) {
        this.publishedCounter = Counter.builder("oms_events_published_total")
                .tag("transport", "noop")
                .description("Domain events handed to the publisher")
                .register(registry);
    }

    @Override
    public void publish(DomainEvent event) {
        publishedCounter.increment();
    }
}
