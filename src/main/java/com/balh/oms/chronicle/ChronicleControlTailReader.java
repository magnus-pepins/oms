package com.balh.oms.chronicle;

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

import java.time.Instant;
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
 *
 * <p><strong>Topology:</strong> {@code ChronicleQueue.createTailer(id)} persists {@code id} under {@code oms.chronicle.queue-dir}.
 * Run <strong>one</strong> active {@code ChronicleControlTailReader} per shared queue directory in production unless
 * operations intentionally assign distinct {@code oms.chronicle.control-tail-id} values with separate queue paths or
 * a proven shard/partition model. On boot, {@code ExcerptTailer.toStart()} runs only when
 * {@code oms.chronicle.control-tail-replay-from-start-on-boot} is {@code true} (default); when {@code false}, the
 * reader resumes the persisted index for {@code oms.chronicle.control-tail-id}. Parallel JVMs with the same id on the
 * same path risk corrupt tailer state — use one reader or distinct dirs/ids by design.
 */
public class ChronicleControlTailReader implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ChronicleControlTailReader.class);

    /** Upper bound for joining the dedicated tail thread on shutdown. */
    private static final long DEDICATED_THREAD_JOIN_TIMEOUT_MS = 5000L;

    /** Park duration after an unexpected failure in the dedicated loop before retrying. */
    private static final long DEDICATED_LOOP_ERROR_BACKOFF_MS = 1000L;

    private final ChronicleQueue queue;
    private final ControlJournal controlJournal;
    private final ControlTailer controlTailer;
    private final ControlChroniclePayloadCodec controlPayloadCodec;
    private final OmsConfig config;
    private final Counter appliedCounter;
    private final Counter skippedCounter;
    private final Counter staleRejectedCounter;
    private final Counter errorCounter;
    private final Counter pipelineTelemetryCounter;

    private final AtomicBoolean dedicatedStop = new AtomicBoolean(false);

    private ExcerptTailer tailer;
    private Thread dedicatedThread;

    public ChronicleControlTailReader(
            ChronicleQueue queue,
            ControlJournal controlJournal,
            ControlTailer controlTailer,
            ControlChroniclePayloadCodec controlPayloadCodec,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        this.queue = queue;
        this.controlJournal = controlJournal;
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
        this.pipelineTelemetryCounter = Counter.builder("oms_control_chronicle_pipeline_telemetry_total")
                .description("ControlPipelineTelemetry Chronicle messages (observation-only, no CAS)")
                .register(meterRegistry);
    }

    @PostConstruct
    void startTailer() {
        String tailerId = config.getChronicle().getControlTailId();
        this.tailer = queue.createTailer(tailerId);
        boolean replayFromStart = config.getChronicle().isControlTailReplayFromStartOnBoot();
        if (replayFromStart) {
            this.tailer.toStart();
        }
        if (config.getChronicle().getTailDriver() == ChronicleTailDriver.DEDICATED) {
            this.dedicatedThread = new Thread(this::runDedicatedLoop, "oms-chronicle-control-tail");
            this.dedicatedThread.setDaemon(false);
            this.dedicatedThread.start();
            log.info(
                    "Chronicle control tail tailerId={} replayFromStartOnBoot={} driver=DEDICATED idleParkNanos={} batchMax={}",
                    tailerId,
                    replayFromStart,
                    config.getChronicle().getTailDedicatedIdleParkNanos(),
                    config.getChronicle().getTailBatchMaxMessages());
        } else {
            log.info(
                    "Chronicle control tail tailerId={} replayFromStartOnBoot={} driver=SCHEDULED pollIntervalMs={} batchMax={}",
                    tailerId,
                    replayFromStart,
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
            if (ControlChronicleEventTypes.CONTROL_PIPELINE_TELEMETRY.equals(event.type())) {
                pipelineTelemetryCounter.increment();
                if (log.isTraceEnabled()) {
                    log.trace(
                            "pipeline telemetry orderId={} hops={}",
                            event.orderId(),
                            event.telemetryHops().size());
                }
                return;
            }
            if (!ControlChronicleEventTypes.ORDER_ACCEPTED.equals(event.type())) {
                log.warn("Ignoring unknown Chronicle control type: {}", event.type());
                skippedCounter.increment();
                return;
            }
            ControlTailer.TailResult r = controlTailer.apply(event);
            switch (r) {
                case APPLIED -> {
                    appliedCounter.increment();
                    appendControlPipelineTelemetry(event);
                }
                case STALE_REJECTED -> staleRejectedCounter.increment();
                case SKIPPED_VERSION_MISMATCH, UNKNOWN_ORDER -> skippedCounter.increment();
                case BUYING_POWER_REJECTED, LEDGER_SERVICE_REJECTED, RISK_PIPELINE_REJECTED -> skippedCounter.increment();
            }
        } catch (Exception e) {
            errorCounter.increment();
            log.error("Failed to apply Chronicle control payload", e);
        }
    }

    private void appendControlPipelineTelemetry(PendingControlEvent appliedOrderAccepted) {
        try {
            byte[] bytes = controlPayloadCodec.chronicleAppendAfterControlTailApply(appliedOrderAccepted, Instant.now());
            controlJournal.append(bytes);
        } catch (Exception e) {
            log.warn(
                    "Failed to append ControlPipelineTelemetry Chronicle message for orderId={}",
                    appliedOrderAccepted.orderId(),
                    e);
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
