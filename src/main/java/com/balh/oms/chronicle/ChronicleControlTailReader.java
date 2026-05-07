package com.balh.oms.chronicle;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.tailer.ControlTailer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Reads committed control payloads from Chronicle and applies them through
 * {@link ControlTailer} (CAS on {@code orders.version}).
 *
 * <p>This closes the slice-1 loop: {@code OutboxReconciler} appends after Postgres
 * commit; this component consumes the journal and mutates {@code orders} in a
 * second, idempotent step.
 */
public class ChronicleControlTailReader {

    private static final Logger log = LoggerFactory.getLogger(ChronicleControlTailReader.class);

    private static final String EVENT_ORDER_ACCEPTED = "OrderAccepted";

    private final ChronicleQueue queue;
    private final ControlTailer controlTailer;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final Counter appliedCounter;
    private final Counter skippedCounter;
    private final Counter staleRejectedCounter;
    private final Counter errorCounter;

    private ExcerptTailer tailer;

    public ChronicleControlTailReader(
            ChronicleQueue queue,
            ControlTailer controlTailer,
            ObjectMapper objectMapper,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.queue = queue;
        this.controlTailer = controlTailer;
        this.objectMapper = objectMapper;
        this.config = config;
        this.appliedCounter = Counter.builder("oms_control_chronicle_tail_applied_total")
                .description("Chronicle control messages applied to Postgres via CAS")
                .register(meterRegistry);
        this.skippedCounter = Counter.builder("oms_control_chronicle_tail_skipped_total")
                .description("Chronicle control messages skipped (unknown type or CAS no-op)")
                .register(meterRegistry);
        this.staleRejectedCounter = Counter.builder("oms_control_chronicle_tail_stale_rejected_total")
                .description("Chronicle control messages that became stale and were rejected")
                .register(meterRegistry);
        this.errorCounter = Counter.builder("oms_control_chronicle_tail_errors_total")
                .description("Chronicle control tail parse/apply failures")
                .register(meterRegistry);
    }

    @PostConstruct
    void startTailer() {
        this.tailer = queue.createTailer("oms-control");
        // Replay from the start on each process start so crash recovery does not
        // depend on a durable cursor yet (see class javadoc).
        this.tailer.toStart();
    }

    @Scheduled(fixedDelayString = "${oms.chronicle.tail-poll-interval-ms:50}")
    public void pollBatch() {
        int max = config.getChronicle().getTailBatchMaxMessages();
        for (int i = 0; i < max; i++) {
            boolean read = tailer.readBytes(bytes -> {
                int len = Math.toIntExact(bytes.readRemaining());
                byte[] payload = new byte[len];
                bytes.read(payload);
                dispatch(payload);
            });
            if (!read) {
                break;
            }
        }
    }

    private void dispatch(byte[] payload) {
        try {
            PendingControlEvent event = objectMapper.readValue(payload, PendingControlEvent.class);
            if (!EVENT_ORDER_ACCEPTED.equals(event.type())) {
                log.warn("Ignoring unknown Chronicle control type: {}", event.type());
                skippedCounter.increment();
                return;
            }
            ControlTailer.TailResult r = controlTailer.apply(event);
            switch (r) {
                case APPLIED -> appliedCounter.increment();
                case STALE_REJECTED -> staleRejectedCounter.increment();
                case SKIPPED_VERSION_MISMATCH -> skippedCounter.increment();
            }
        } catch (Exception e) {
            errorCounter.increment();
            log.error("Failed to apply Chronicle control payload", e);
        }
    }
}
