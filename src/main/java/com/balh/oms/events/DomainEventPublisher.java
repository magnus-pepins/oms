package com.balh.oms.events;

/**
 * Publishes domain events to the fanout bus (NATS in production).
 *
 * <p>Slice 1 ships a no-op default ({@link NoOpDomainEventPublisher}) so we can
 * exercise the controller end-to-end without standing up NATS. The NATS-backed
 * implementation lands in slice 1.5 alongside the desk live feed and external
 * drop copy.
 *
 * <p>Implementations MUST only be invoked AFTER the originating Postgres
 * transaction commits.
 */
public interface DomainEventPublisher {
    void publish(DomainEvent event);
}
