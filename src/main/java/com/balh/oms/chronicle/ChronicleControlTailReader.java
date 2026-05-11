package com.balh.oms.chronicle;

import com.balh.oms.chronicle.ControlChroniclePayloadCodec;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.tailer.ControlTailer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Reads committed control payloads from Chronicle and applies them through
 * {@link ControlTailer} (CAS on {@code orders.version}).
 *
 * <p>This closes the slice-1 loop: {@code OutboxReconciler} appends after Postgres
 * commit; this component consumes the journal and mutates {@code orders} in a
 * second, idempotent step.
 *
 * <p>Tail consumption is driven either by Spring scheduling ({@link ChronicleTailDriver#SCHEDULED})
 * or by a dedicated thread ({@link ChronicleTailDriver#DEDICATED}); see {@code docs/chronicle-tail-driver.md}.
 */
public class ChronicleControlTailReader implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ChronicleControlTailReader.class);

    private static final String EVENT_ORDER_ACCEPTED = "OrderAccepted";

    /** Upper bound for joining the dedicated tail thread on shutdown. */
    private static final long DEDICATED_THREAD_JOIN_TIMEOUT_MS = 5000L;

    /** Park duration after an unexpected failure in the dedicated loop before retrying. */
    private static final long DEDICATED_LOOP_ERROR_BACKOFF_MS = 1000L;

    private final ChronicleQueue queue;
    private final ControlTailer controlTailer;
    private final ControlChroniclePayloadCodec controlPayloadCodec;
    private final OmsConfig config;
    private final Counter appliedCounter;
    private final Counter skippedCounter;
    private final Counter staleRejectedCounter;
    private final Counter errorCounter;

    private final AtomicBoolean dedicatedStop = new AtomicBoolean(false);

    private ExcerptTailer tailer;
    private Thread dedicatedThread;

    public ChronicleControlTailReader(
            ChronicleQueue queue,
            ControlTailer controlTailer,
            ControlChroniclePayloadCodec controlPayloadCodec,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.queue = queue;
        this.controlTailer = controlTailer;
        this.controlPayloadCodec = controlPayloadCodec;
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
        if (config.getChronicle().getTailDriver() == ChronicleTailDriver.DEDICATED) {
            this.dedicatedThread = new Thread(this::runDedicatedLoop, "oms-chronicle-control-tail");
            this.dedicatedThread.setDaemon(false);
            this.dedicatedThread.start();
            log.info(
                    "Chronicle control tail driver=DEDICATED idleParkNanos={} batchMax={}",
                    config.getChronicle().getTailDedicatedIdleParkNanos(),
                    config.getChronicle().getTailBatchMaxMessages());
        } else {
            log.info(
                    "Chronicle control tail driver=SCHEDULED pollIntervalMs={} batchMax={}",
                    config.getChronicle().getTailPollIntervalMs(),
                    config.getChronicle().getTailBatchMaxMessages());
        }
    }

    /**
     * One scheduled tick: drain up to {@link OmsConfig.Chronicle#getTailBatchMaxMessages()} messages.
     * Used only when {@link ChronicleTailDriver#SCHEDULED} (see {@code ChronicleQueueConfiguration#chronicleControlTailPollScheduling}).
     */
    public void pollBatch() {
        int max = config.getChronicle().getTailBatchMaxMessages();
        for (int i = 0; i < max; i++) {
            if (!readAndDispatchOne()) {
                break;
            }
        }
    }

    private void runDedicatedLoop() {
        while (!dedicatedStop.get()) {
            try {
                boolean drainedAny = false;
                int max = config.getChronicle().getTailBatchMaxMessages();
                for (int i = 0; i < max; i++) {
                    if (!readAndDispatchOne()) {
                        break;
                    }
                    drainedAny = true;
                }
                if (!drainedAny) {
                    long parkNanos = config.getChronicle().getTailDedicatedIdleParkNanos();
                    if (parkNanos > 0L) {
                        LockSupport.parkNanos(parkNanos);
                    }
                }
            } catch (Throwable t) {
                log.error("Dedicated Chronicle control tail loop failed; backing off", t);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DEDICATED_LOOP_ERROR_BACKOFF_MS));
            }
        }
    }

    private boolean readAndDispatchOne() {
        return tailer.readBytes(bytes -> {
            int len = Math.toIntExact(bytes.readRemaining());
            byte[] payload = new byte[len];
            bytes.read(payload);
            dispatch(payload);
        });
    }

    private void dispatch(byte[] payload) {
        try {
            PendingControlEvent event = controlPayloadCodec.decodeChronicleExcerpt(payload);
            if (!EVENT_ORDER_ACCEPTED.equals(event.type())) {
                log.warn("Ignoring unknown Chronicle control type: {}", event.type());
                skippedCounter.increment();
                return;
            }
            ControlTailer.TailResult r = controlTailer.apply(event);
            switch (r) {
                case APPLIED -> appliedCounter.increment();
                case STALE_REJECTED -> staleRejectedCounter.increment();
                case SKIPPED_VERSION_MISMATCH, UNKNOWN_ORDER -> skippedCounter.increment();
                case BUYING_POWER_REJECTED, LEDGER_SERVICE_REJECTED, RISK_PIPELINE_REJECTED -> skippedCounter.increment();
            }
        } catch (Exception e) {
            errorCounter.increment();
            log.error("Failed to apply Chronicle control payload", e);
        }
    }

    @Override
    public void destroy() {
        if (dedicatedThread == null) {
            return;
        }
        dedicatedStop.set(true);
        dedicatedThread.interrupt();
        try {
            dedicatedThread.join(DEDICATED_THREAD_JOIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while joining dedicated Chronicle control tail thread");
        }
        if (dedicatedThread.isAlive()) {
            log.warn(
                    "Dedicated Chronicle control tail thread did not stop within {} ms",
                    DEDICATED_THREAD_JOIN_TIMEOUT_MS);
        }
    }
}
