package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.events.FanoutClient;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Drains {@link DomainEventOutboxRepository} after Postgres commit and hands
 * envelopes to {@link FanoutClient} (NATS or no-op). Each tick runs in one DB transaction: pending rows are
 * claimed with {@code FOR UPDATE SKIP LOCKED} so multiple reconciler JVMs do not fetch the same row before mark.
 *
 * <p>Only runs when {@code oms.events.nats.enabled=true}, i.e. when a real {@link FanoutClient}
 * (NATS) is wired. Processes that fall back to {@code NoOpFanoutClient} (NATS disabled, e.g.
 * {@code oms-ingress-replica}) MUST NOT drain the shared outbox: {@code NoOpFanoutClient.deliver()}
 * returns {@code true} without sending, so a NoOp reconciler racing a NATS reconciler for the same
 * row via {@code SKIP LOCKED} would mark it published and silently destroy the event — leaving the
 * customer order stuck at "Awaiting Approval". Gating here makes the NATS-enabled process(es) the
 * sole drainer(s).
 */
@Component
@ConditionalOnProperty(prefix = "oms.events.nats", name = "enabled", havingValue = "true")
public class DomainFanoutReconciler {

    private static final String METRIC_OUTBOX_PUBLISHED = "oms_domain_fanout_outbox_published_total";
    private static final String METRIC_OUTBOX_FAILED = "oms_domain_fanout_outbox_failed_total";
    private static final String METRIC_DELIVERED_BY_TYPE = "oms_fanout_delivered_total";

    private static final Logger log = LoggerFactory.getLogger(DomainFanoutReconciler.class);

    private final DomainEventOutboxRepository outbox;
    private final FanoutClient fanout;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public DomainFanoutReconciler(
            DomainEventOutboxRepository outbox,
            FanoutClient fanout,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.fanout = fanout;
        this.config = config;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.domain-events.reconciler-interval-ms:500}")
    public void runOnce() {
        transactionTemplate.executeWithoutResult(status -> {
            Instant olderThan = Instant.now().minus(
                    config.getDomainEvents().getReconcilerAgeMs(), ChronoUnit.MILLIS);
            List<DomainEventOutboxRepository.FanoutRow> rows =
                    outbox.fetchPendingOlderThan(olderThan, config.getDomainEvents().getReconcilerBatchSize());
            for (var row : rows) {
                try {
                    boolean ok = fanout.deliver(row.envelopeJson());
                    if (ok) {
                        outbox.markPublished(row.id(), Instant.now());
                        meterRegistry.counter(METRIC_OUTBOX_PUBLISHED).increment();
                        meterRegistry.counter(METRIC_DELIVERED_BY_TYPE, "event_type", readType(row.envelopeJson()))
                                .increment();
                    } else {
                        meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                        outbox.markFailed(row.id(), "fanout_deliver_returned_false", Instant.now());
                        log.warn("Domain fanout deliver returned false for outbox id={}", row.id());
                    }
                } catch (Exception e) {
                    meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                    outbox.markFailed(row.id(), e.toString(), Instant.now());
                    log.warn("Domain fanout deliver failed for outbox id={} (attempt {})",
                            row.id(), row.attempts() + 1, e);
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
