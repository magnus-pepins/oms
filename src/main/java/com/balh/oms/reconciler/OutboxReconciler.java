package com.balh.oms.reconciler;

import com.balh.oms.chronicle.ControlChroniclePayloadCodec;
import com.balh.oms.chronicle.ControlJournal;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.ControlOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Drives the slice-1 mandatory invariant: <strong>append to Chronicle only
 * after Postgres commit</strong>.
 *
 * <p>Picks up rows from {@link ControlOutboxRepository} where
 * {@code chronicle_enqueued_at IS NULL}, appends them to the
 * {@link ControlJournal}, and marks them as appended. Failures are recorded on
 * the row (attempts, last_error) so the next invocation will retry; the row
 * remains in {@code control_outbox} until success.
 *
 * <p>Cadence is controlled by {@code oms.outbox.reconciler-interval-ms}; the
 * minimum age before a row is eligible is {@code oms.outbox.reconciler-age-ms}
 * to avoid contention with the just-committed transaction's own publisher.
 *
 * <p>Each {@link #runOnce()} invocation runs in a <strong>single database transaction</strong> and claims pending
 * rows with {@code FOR UPDATE SKIP LOCKED}, so more than one reconciler instance can drain the same outbox safely
 * (topology: N ingress + shared Postgres + multiple control workers using {@code reconciler} append mode).
 */
@Component
public class OutboxReconciler {

    private static final Logger log = LoggerFactory.getLogger(OutboxReconciler.class);

    private final ControlOutboxRepository outbox;
    private final ControlJournal journal;
    private final ControlChroniclePayloadCodec controlPayloadCodec;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final Counter appended;
    private final Counter failed;

    public OutboxReconciler(
            ControlOutboxRepository outbox,
            ControlJournal journal,
            ControlChroniclePayloadCodec controlPayloadCodec,
            OmsConfig config,
            MeterRegistry registry) {
        this.outbox = outbox;
        this.journal = journal;
        this.controlPayloadCodec = controlPayloadCodec;
        this.config = config;
        this.meterRegistry = registry;
        this.appended = Counter.builder("oms_control_outbox_appended_total")
                .description("Outbox rows successfully appended to Chronicle")
                .register(registry);
        this.failed = Counter.builder("oms_control_outbox_failed_total")
                .description("Outbox rows that failed Chronicle append")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${oms.outbox.reconciler-interval-ms:500}")
    @Transactional
    public void runOnce() {
        Instant olderThan = Instant.now().minus(
                config.getOutbox().getReconcilerAgeMs(), ChronoUnit.MILLIS);
        List<ControlOutboxRepository.OutboxRow> rows =
                outbox.fetchPendingOlderThan(olderThan, config.getOutbox().getReconcilerBatchSize());
        for (var row : rows) {
            try {
                Timer.Sample appendSample = Timer.start(meterRegistry);
                journal.append(controlPayloadCodec.chronicleAppendBytesFromOutboxPayloadText(row.payload()));
                Instant appendedAt = Instant.now();
                outbox.markAppended(row.id(), appendedAt);
                OmsPipelineMetrics.finishChronicleAppend(meterRegistry, appendSample);
                Duration lag = Duration.between(row.enqueuedAt(), appendedAt);
                OmsPipelineMetrics.recordOutboxToChronicleLag(meterRegistry, lag);
                appended.increment();
            } catch (Exception e) {
                failed.increment();
                outbox.markFailed(row.id(), e.toString());
                log.warn("Chronicle append failed for outbox id={} (attempt {})",
                        row.id(), row.attempts() + 1, e);
            }
        }
    }
}
