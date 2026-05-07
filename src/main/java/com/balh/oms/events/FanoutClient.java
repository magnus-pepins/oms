package com.balh.oms.events;

/**
 * Delivers transactional-outbox envelopes to the external fanout (NATS, etc.).
 */
public interface FanoutClient {

    /**
     * @param envelopeJson full envelope JSON (see {@link DomainEventEnvelopeCodec})
     * @return {@code false} if delivery failed and the outbox row should be retried
     */
    boolean deliver(String envelopeJson);
}
