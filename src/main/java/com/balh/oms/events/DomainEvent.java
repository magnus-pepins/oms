package com.balh.oms.events;

/**
 * Marker interface for domain events emitted to the fanout bus
 * (NATS / desk SSE / drop copy).
 *
 * <p>Concrete events are records (e.g. {@link OrderAcceptedEvent}). Field
 * naming follows the wire schema described in
 * {@code oms/docs/drop-copy-events.md}.
 */
public interface DomainEvent {
    String type();

    int schemaVersion();
}
