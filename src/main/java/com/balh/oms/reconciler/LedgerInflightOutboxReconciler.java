package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * After Postgres commit, delivers BUY inflight hold rows from {@link LedgerInflightOutboxRepository}
 * to {@link LedgerInflightReservationClient} (idempotent {@code oms:order:{uuid}} reference). Each tick runs in one
 * DB transaction with {@code FOR UPDATE SKIP LOCKED} on the pending fetch so multiple reconciler JVMs cannot
 * claim the same row before mark.
 */
@Component
public class LedgerInflightOutboxReconciler {

    private static final String METRIC_OUTBOX_PUBLISHED = "oms_ledger_inflight_outbox_published_total";
    private static final String METRIC_OUTBOX_FAILED = "oms_ledger_inflight_outbox_failed_total";
    private static final String METRIC_OUTBOX_BULK_PUBLISHED = "oms_ledger_inflight_outbox_bulk_published_total";
    private static final String METRIC_OUTBOX_BULK_FAILED = "oms_ledger_inflight_outbox_bulk_failed_total";
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightOutboxReconciler.class);

    private final LedgerInflightOutboxRepository outbox;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final ObjectProvider<LedgerInflightBulkDispatcher> ledgerInflightBulkDispatcher;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public LedgerInflightOutboxReconciler(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            ObjectProvider<LedgerInflightBulkDispatcher> ledgerInflightBulkDispatcher,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.ledgerInflightReservation = ledgerInflightReservation;
        this.ledgerInflightBulkDispatcher = ledgerInflightBulkDispatcher;
        this.config = config;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.ledger.inflight-outbox-reconciler-interval-ms:500}")
    public void runOnce() {
        if (!config.getLedger().isInflightReservationEnabled()
                || !config.getLedger().isInflightAsyncEnabled()) {
            return;
        }
        boolean bulkEnabled = config.getLedger().isInflightOutboxBulkEnabled();
        LedgerInflightBulkDispatcher bulkDispatcher =
                bulkEnabled ? ledgerInflightBulkDispatcher.getIfAvailable() : null;
        LedgerInflightReservationClient client =
                bulkEnabled && bulkDispatcher != null
                        ? null
                        : ledgerInflightReservation.getIfAvailable();
        if (client == null && bulkDispatcher == null) {
            return;
        }
        transactionTemplate.executeWithoutResult(status -> {
            Instant olderThan = Instant.now().minus(
                    config.getLedger().getInflightOutboxReconcilerAgeMs(), ChronoUnit.MILLIS);
            List<LedgerInflightOutboxRepository.InflightRow> rows =
                    outbox.fetchPendingOlderThan(olderThan, config.getLedger().getInflightOutboxReconcilerBatchSize());
            if (rows.isEmpty()) {
                return;
            }
            if (bulkDispatcher != null) {
                deliverBulk(bulkDispatcher, rows);
            } else {
                deliverPerRow(client, rows);
            }
        });
    }

    private void deliverBulk(
            LedgerInflightBulkDispatcher bulkDispatcher,
            List<LedgerInflightOutboxRepository.InflightRow> rows) {
        List<LedgerInflightBulkDispatcher.HoldItem> items = new ArrayList<>(rows.size());
        Map<UUID, LedgerInflightOutboxRepository.InflightRow> rowByOrderId = new HashMap<>(rows.size());
        for (var row : rows) {
            ParsedHold parsed;
            try {
                parsed = parseHold(row);
            } catch (Exception e) {
                meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                outbox.markFailed(row.id(), e.toString(), Instant.now());
                log.warn(
                        "Ledger inflight outbox id={} orderId={} has invalid payload",
                        row.id(),
                        row.orderId(),
                        e);
                continue;
            }
            if (!parsed.valid()) {
                meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                outbox.markFailed(row.id(), "invalid_payload_json", Instant.now());
                log.warn("Ledger inflight outbox id={} orderId={} has invalid payload", row.id(), row.orderId());
                continue;
            }
            items.add(
                    new LedgerInflightBulkDispatcher.HoldItem(
                            row.orderId(), parsed.balanceId(), parsed.holdAmount()));
            rowByOrderId.put(row.orderId(), row);
        }
        if (items.isEmpty()) {
            return;
        }
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            LedgerInflightBulkDispatcher.Result result = bulkDispatcher.dispatch(items);
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger bulk inflight hold HTTP call (outbox reconciler slice 4r)")
                    .tag("result", result.failedOrderIds().isEmpty() ? "success" : "partial")
                    .tag("path", "outbox_bulk")
                    .register(meterRegistry));
            Instant now = Instant.now();
            for (LedgerInflightBulkDispatcher.HoldItem item : items) {
                LedgerInflightOutboxRepository.InflightRow row = rowByOrderId.get(item.orderId());
                if (row == null) {
                    continue;
                }
                if (result.failedOrderIds().contains(item.orderId())) {
                    meterRegistry.counter(METRIC_OUTBOX_BULK_FAILED).increment();
                    outbox.markFailed(row.id(), "ledger_bulk_partial_failure", now);
                    log.warn(
                            "Ledger inflight outbox bulk partial failure for id={} orderId={} (attempt {})",
                            row.id(),
                            row.orderId(),
                            row.attempts() + 1);
                } else {
                    outbox.markPublished(row.id(), now);
                    meterRegistry.counter(METRIC_OUTBOX_BULK_PUBLISHED).increment();
                    meterRegistry.counter(METRIC_OUTBOX_PUBLISHED).increment();
                }
            }
        } catch (LedgerInflightBulkDispatcher.LedgerInflightBulkException e) {
            sample.stop(Timer.builder(METRIC_LEDGER_INFLIGHT_HOLD)
                    .description("Ledger bulk inflight hold HTTP call (outbox reconciler slice 4r)")
                    .tag("result", "failure")
                    .tag("path", "outbox_bulk")
                    .register(meterRegistry));
            Instant now = Instant.now();
            for (LedgerInflightOutboxRepository.InflightRow row : rowByOrderId.values()) {
                meterRegistry.counter(METRIC_OUTBOX_BULK_FAILED).increment();
                meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                outbox.markFailed(row.id(), e.toString(), now);
                log.warn(
                        "Ledger inflight outbox bulk deliver failed for id={} orderId={} (attempt {})",
                        row.id(),
                        row.orderId(),
                        row.attempts() + 1,
                        e);
            }
        }
    }

    private void deliverPerRow(
            LedgerInflightReservationClient client,
            List<LedgerInflightOutboxRepository.InflightRow> rows) {
        for (var row : rows) {
            try {
                ParsedHold parsed = parseHold(row);
                if (!parsed.valid()) {
                    meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                    outbox.markFailed(row.id(), "invalid_payload_json", Instant.now());
                    log.warn("Ledger inflight outbox id={} orderId={} has invalid payload", row.id(), row.orderId());
                    continue;
                }
                UUID orderId = row.orderId();
                Timer.Sample sample = Timer.start(meterRegistry);
                String ledgerTxnId;
                try {
                    ledgerTxnId =
                            client.placeBuyFundsHold(orderId, parsed.balanceId(), parsed.holdAmount());
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
                if (ledgerTxnId != null) {
                    outbox.setLedgerTxnId(row.id(), ledgerTxnId);
                }
                meterRegistry.counter(METRIC_OUTBOX_PUBLISHED).increment();
            } catch (Exception e) {
                meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                outbox.markFailed(row.id(), e.toString(), Instant.now());
                log.warn(
                        "Ledger inflight outbox deliver failed for id={} orderId={} (attempt {})",
                        row.id(),
                        row.orderId(),
                        row.attempts() + 1,
                        e);
            }
        }
    }

    private ParsedHold parseHold(LedgerInflightOutboxRepository.InflightRow row) throws Exception {
        JsonNode root = objectMapper.readTree(row.payloadJson());
        String balanceId = root.path("ledgerBalanceId").asText("").trim();
        String holdText = root.path("holdAmount").asText("").trim();
        BigDecimal holdAmount = null;
        if (!holdText.isEmpty()) {
            holdAmount = new BigDecimal(holdText);
        } else {
            String qtyText = root.path("quantity").asText("").trim();
            String limitText = root.path("limitPrice").asText("").trim();
            if (!balanceId.isEmpty() && !qtyText.isEmpty() && !limitText.isEmpty()) {
                holdAmount = new BigDecimal(qtyText).multiply(new BigDecimal(limitText));
                String feeText = root.path("feeAmount").asText("").trim();
                if (!feeText.isEmpty()) {
                    holdAmount = holdAmount.add(new BigDecimal(feeText));
                }
            }
        }
        return new ParsedHold(balanceId, holdAmount);
    }

    private record ParsedHold(String balanceId, BigDecimal holdAmount) {
        boolean valid() {
            return balanceId != null
                    && !balanceId.isEmpty()
                    && holdAmount != null
                    && holdAmount.signum() > 0;
        }
    }
}
