package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * After Postgres commit, delivers BUY inflight hold rows from {@link LedgerInflightOutboxRepository}
 * to {@link LedgerInflightReservationClient} (idempotent {@code oms:order:{uuid}} reference).
 */
@Component
public class LedgerInflightOutboxReconciler {

    private static final String METRIC_OUTBOX_PUBLISHED = "oms_ledger_inflight_outbox_published_total";
    private static final String METRIC_OUTBOX_FAILED = "oms_ledger_inflight_outbox_failed_total";
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightOutboxReconciler.class);

    private final LedgerInflightOutboxRepository outbox;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public LedgerInflightOutboxReconciler(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.outbox = outbox;
        this.ledgerInflightReservation = ledgerInflightReservation;
        this.config = config;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(fixedDelayString = "${oms.ledger.inflight-outbox-reconciler-interval-ms:500}")
    public void runOnce() {
        if (!config.getLedger().isInflightReservationEnabled()
                || !config.getLedger().isInflightAsyncEnabled()) {
            return;
        }
        LedgerInflightReservationClient client = ledgerInflightReservation.getIfAvailable();
        if (client == null) {
            return;
        }
        Instant olderThan = Instant.now().minus(
                config.getLedger().getInflightOutboxReconcilerAgeMs(), ChronoUnit.MILLIS);
        List<LedgerInflightOutboxRepository.InflightRow> rows =
                outbox.fetchPendingOlderThan(olderThan, config.getLedger().getInflightOutboxReconcilerBatchSize());
        for (var row : rows) {
            try {
                JsonNode root = objectMapper.readTree(row.payloadJson());
                String balanceId = root.path("ledgerBalanceId").asText("").trim();
                String qtyText = root.path("quantity").asText("").trim();
                String limitText = root.path("limitPrice").asText("").trim();
                if (balanceId.isEmpty() || qtyText.isEmpty() || limitText.isEmpty()) {
                    meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                    outbox.markFailed(row.id(), "invalid_payload_json", Instant.now());
                    log.warn("Ledger inflight outbox id={} orderId={} has invalid payload", row.id(), row.orderId());
                    continue;
                }
                BigDecimal quantity = new BigDecimal(qtyText);
                BigDecimal limitPrice = new BigDecimal(limitText);
                UUID orderId = row.orderId();
                Timer.Sample sample = Timer.start(meterRegistry);
                try {
                    client.placeBuyNotionalHold(orderId, balanceId, quantity, limitPrice);
                    sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                            .description("Ledger sync inflight hold HTTP call (sync path or outbox reconciler)")
                            .tag("result", "success")
                            .tag("path", "outbox")
                            .register(meterRegistry));
                } catch (LedgerInflightReservationClient.LedgerReservationException e) {
                    sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                            .description("Ledger sync inflight hold HTTP call (sync path or outbox reconciler)")
                            .tag("result", "failure")
                            .tag("path", "outbox")
                            .register(meterRegistry));
                    throw e;
                }
                outbox.markPublished(row.id(), Instant.now());
                meterRegistry.counter(METRIC_OUTBOX_PUBLISHED).increment();
            } catch (Exception e) {
                meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                outbox.markFailed(row.id(), e.toString(), Instant.now());
                log.warn("Ledger inflight outbox deliver failed for id={} orderId={} (attempt {})",
                        row.id(), row.orderId(), row.attempts() + 1, e);
            }
        }
    }
}
