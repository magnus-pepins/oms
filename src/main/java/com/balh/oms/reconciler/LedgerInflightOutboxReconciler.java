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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
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
    private static final String METRIC_LEDGER_INFLIGHT_HOLD = "oms_ledger_inflight_hold";

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightOutboxReconciler.class);

    private final LedgerInflightOutboxRepository outbox;
    private final ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public LedgerInflightOutboxReconciler(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<LedgerInflightReservationClient> ledgerInflightReservation,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.ledgerInflightReservation = ledgerInflightReservation;
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
        LedgerInflightReservationClient client = ledgerInflightReservation.getIfAvailable();
        if (client == null) {
            return;
        }
        transactionTemplate.executeWithoutResult(status -> {
            Instant olderThan = Instant.now().minus(
                    config.getLedger().getInflightOutboxReconcilerAgeMs(), ChronoUnit.MILLIS);
            List<LedgerInflightOutboxRepository.InflightRow> rows =
                    outbox.fetchPendingOlderThan(olderThan, config.getLedger().getInflightOutboxReconcilerBatchSize());
            for (var row : rows) {
                try {
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
                    if (balanceId.isEmpty() || holdAmount == null || holdAmount.signum() <= 0) {
                        meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                        outbox.markFailed(row.id(), "invalid_payload_json", Instant.now());
                        log.warn("Ledger inflight outbox id={} orderId={} has invalid payload", row.id(), row.orderId());
                        continue;
                    }
                    UUID orderId = row.orderId();
                    Timer.Sample sample = Timer.start(meterRegistry);
                    String ledgerTxnId;
                    try {
                        ledgerTxnId = client.placeBuyFundsHold(orderId, balanceId, holdAmount);
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
                    // Wed-demo (V32): persist the Ledger-returned txn_<uuid> so the lifecycle
                    // reconciler (commit-on-fill / void-on-cancel) can address the hold via
                    // PUT /transactions/inflight/{txID}. Idempotent (setLedgerTxnId returns
                    // false if the row already has a non-null id; that's the desired outcome on
                    // a publish retry). A null id from the client (parse failure) leaves the row
                    // unsettleable — Ledger expiry sweep is the safety net.
                    if (ledgerTxnId != null) {
                        outbox.setLedgerTxnId(row.id(), ledgerTxnId);
                    }
                    meterRegistry.counter(METRIC_OUTBOX_PUBLISHED).increment();
                } catch (Exception e) {
                    meterRegistry.counter(METRIC_OUTBOX_FAILED).increment();
                    outbox.markFailed(row.id(), e.toString(), Instant.now());
                    log.warn("Ledger inflight outbox deliver failed for id={} orderId={} (attempt {})",
                            row.id(), row.orderId(), row.attempts() + 1, e);
                }
            }
        });
    }
}
