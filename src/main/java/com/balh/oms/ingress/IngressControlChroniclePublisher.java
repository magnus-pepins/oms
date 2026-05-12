package com.balh.oms.ingress;

import com.balh.oms.chronicle.ControlChronicleAppendMode;
import com.balh.oms.chronicle.ControlChroniclePayloadCodec;
import com.balh.oms.chronicle.ControlJournal;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.ControlOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * When {@code oms.control.chronicle-append-mode} is {@value ControlChronicleAppendMode#INGRESS_AFTER_COMMIT}, appends
 * to Chronicle and marks {@code control_outbox} after the ingress transaction commits (see topology plan P2).
 */
@Component
@ConditionalOnProperty(prefix = "oms.control", name = "chronicle-append-mode", havingValue = ControlChronicleAppendMode.INGRESS_AFTER_COMMIT)
public class IngressControlChroniclePublisher {

    private static final Logger log = LoggerFactory.getLogger(IngressControlChroniclePublisher.class);

    private final ControlJournal journal;
    private final ControlChroniclePayloadCodec codec;
    private final ControlOutboxRepository outbox;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate markAppendedTx;
    private final Counter successCounter;
    private final Counter failedCounter;

    public IngressControlChroniclePublisher(
            ControlJournal journal,
            ControlChroniclePayloadCodec codec,
            ControlOutboxRepository outbox,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.journal = journal;
        this.codec = codec;
        this.outbox = outbox;
        this.meterRegistry = meterRegistry;
        this.markAppendedTx = new TransactionTemplate(transactionManager);
        this.markAppendedTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.successCounter = Counter.builder("oms_control_outbox_ingress_chronicle_append_success_total")
                .description("control_outbox rows appended to Chronicle from ingress afterCommit hook")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("oms_control_outbox_ingress_chronicle_append_failed_total")
                .description("Failures appending or marking control_outbox from ingress afterCommit hook")
                .register(meterRegistry);
    }

    public void appendMarkMetrics(long outboxId, String outboxPayloadText, Instant controlOutboxEnqueuedAt) {
        Timer.Sample appendSample = Timer.start(meterRegistry);
        try {
            byte[] excerpt = codec.chronicleAppendBytesFromOutboxPayloadText(outboxPayloadText);
            journal.append(excerpt);
            Instant appendedAt = Instant.now();
            markAppendedTx.executeWithoutResult(status -> outbox.markAppended(outboxId, appendedAt));
            Duration lag = Duration.between(controlOutboxEnqueuedAt, appendedAt);
            OmsPipelineMetrics.recordOutboxToChronicleLag(meterRegistry, lag);
            OmsPipelineMetrics.finishChronicleAppend(meterRegistry, appendSample);
            successCounter.increment();
        } catch (Exception e) {
            failedCounter.increment();
            log.error("Ingress-after-commit Chronicle append failed for control_outbox id={}", outboxId, e);
        }
    }
}
