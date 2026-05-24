package com.balh.oms.settlement;

/**
 * Delivers {@code settlement_customer_notification_outbox} envelopes to the customer BFF
 * (NATS JetStream when enabled).
 */
public interface CustomerNotificationPublisher {

    /**
     * @return {@code true} when the transport accepted the envelope for delivery
     */
    boolean deliver(String envelopeJson);
}
