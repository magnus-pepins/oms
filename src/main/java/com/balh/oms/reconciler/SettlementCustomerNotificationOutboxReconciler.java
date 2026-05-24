package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.settlement.CustomerNotificationPublisher;
import com.balh.oms.settlement.SettlementCustomerNotificationOutboxRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Drains {@code settlement_customer_notification_outbox} to NATS for the customer BFF (§5.8 / §5.9).
 */
@Component
@ConditionalOnProperty(name = "oms.settlement.customer-notification-publisher-enabled", havingValue = "true")
public class SettlementCustomerNotificationOutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(SettlementCustomerNotificationOutboxReconciler.class);

    private static final String METRIC_PUBLISHED = "oms_settlement_customer_notification_outbox_published_total";
    private static final String METRIC_FAILED = "oms_settlement_customer_notification_outbox_failed_total";
    private static final String METRIC_DELIVERED_BY_TYPE = "oms_settlement_customer_notification_published_total";

    private final SettlementCustomerNotificationOutboxRepository outbox;
    private final CustomerNotificationPublisher publisher;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public SettlementCustomerNotificationOutboxReconciler(
            SettlementCustomerNotificationOutboxRepository outbox,
            CustomerNotificationPublisher publisher,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.config = config;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(
            fixedDelayString = "${oms.settlement.customer-notification-publisher-interval-ms:1000}")
    public void runOnce() {
        transactionTemplate.executeWithoutResult(
                status -> {
                    Instant olderThan =
                            Instant.now()
                                    .minus(
                                            config.getSettlement().getCustomerNotificationPublisherAgeMs(),
                                            ChronoUnit.MILLIS);
                    List<SettlementCustomerNotificationOutboxRepository.OutboxRow> rows =
                            outbox.fetchPendingOlderThan(
                                    olderThan,
                                    config.getSettlement().getCustomerNotificationPublisherBatchSize());
                    for (var row : rows) {
                        try {
                            boolean ok = publisher.deliver(row.envelopeJson());
                            if (ok) {
                                outbox.markPublished(row.id(), Instant.now());
                                meterRegistry.counter(METRIC_PUBLISHED).increment();
                                meterRegistry
                                        .counter(
                                                METRIC_DELIVERED_BY_TYPE,
                                                "notification_type",
                                                readType(row.envelopeJson()))
                                        .increment();
                            } else {
                                meterRegistry.counter(METRIC_FAILED).increment();
                                outbox.markFailed(row.id(), "publisher_deliver_returned_false");
                                log.warn(
                                        "settlement customer notification deliver returned false id={}",
                                        row.id());
                            }
                        } catch (Exception e) {
                            meterRegistry.counter(METRIC_FAILED).increment();
                            outbox.markFailed(row.id(), e.toString());
                            log.warn(
                                    "settlement customer notification deliver failed id={} (attempt {})",
                                    row.id(),
                                    row.attempts() + 1,
                                    e);
                        }
                    }
                });
    }

    private String readType(String envelopeJson) {
        try {
            JsonNode n = objectMapper.readTree(envelopeJson);
            return n.path("type").asText("unknown");
        } catch (Exception e) {
            return "parse_error";
        }
    }
}
