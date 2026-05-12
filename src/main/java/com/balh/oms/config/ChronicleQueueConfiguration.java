package com.balh.oms.config;

import com.balh.oms.chronicle.ChronicleControlJournal;
import com.balh.oms.chronicle.ChronicleControlTailReader;
import com.balh.oms.chronicle.ControlChroniclePayloadCodec;
import com.balh.oms.tailer.ControlTailer;
import io.micrometer.core.instrument.MeterRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.SchedulingConfigurer;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Registers the shared {@link ChronicleQueue} plus append-only journal and
 * optionally {@link ChronicleControlTailReader} (scheduled or dedicated tail driver) when
 * {@code oms.chronicle.control-tail-enabled=true}. Disabled in the {@code test} profile (tests use
 * {@link com.balh.oms.chronicle.NoOpControlJournal} via {@link ControlJournalFallbackConfiguration} instead).
 *
 * <p>Slice 1 tail policy: the tailer starts at the beginning of the queue on
 * process start so any message appended before a crash is still applied
 * idempotently via CAS. High-volume deployments should add a durable
 * checkpoint (v1.5+) instead of replaying from index zero.
 */
@Configuration
@Profile("!test")
@ConditionalOnProperty(prefix = "oms.chronicle", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ChronicleQueueConfiguration {

    private static final long MIN_TAIL_POLL_INTERVAL_MS = 1L;

    @Bean(destroyMethod = "close")
    public ChronicleQueue controlChronicleQueue(OmsConfig config) {
        Path dir = Path.of(config.getChronicle().getQueueDir());
        var builder = ChronicleQueue.singleBuilder(dir);
        builder.rollCycle(resolveRollCycle(config.getChronicle().getRollCycle()));
        return builder.build();
    }

    @Bean
    public ChronicleControlJournal chronicleControlJournal(ChronicleQueue controlChronicleQueue) {
        return new ChronicleControlJournal(controlChronicleQueue);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.chronicle", name = "control-tail-enabled", havingValue = "true", matchIfMissing = true)
    public ChronicleControlTailReader chronicleControlTailReader(
            ChronicleQueue controlChronicleQueue,
            ChronicleControlJournal chronicleControlJournal,
            ControlTailer controlTailer,
            ControlChroniclePayloadCodec controlChroniclePayloadCodec,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        return new ChronicleControlTailReader(
                controlChronicleQueue,
                chronicleControlJournal,
                controlTailer,
                controlChroniclePayloadCodec,
                config,
                meterRegistry);
    }

    /**
     * Registers fixed-delay {@link ChronicleControlTailReader#pollBatch()} when {@code tail-driver=scheduled}.
     * Defined here (not a separate {@code @Configuration} with {@code @ConditionalOnBean}) so the bean is always
     * created after {@link #chronicleControlTailReader}; {@code @ConditionalOnBean(ChronicleControlTailReader)} on
     * another configuration class can evaluate too early and skip scheduling entirely.
     */
    @Bean
    @ConditionalOnBean(ChronicleControlTailReader.class)
    @ConditionalOnProperty(prefix = "oms.chronicle", name = "tail-driver", havingValue = "scheduled", matchIfMissing = true)
    public SchedulingConfigurer chronicleControlTailPollScheduling(
            ChronicleControlTailReader chronicleControlTailReader,
            OmsConfig omsConfig) {
        return registrar -> {
            long ms = Math.max(MIN_TAIL_POLL_INTERVAL_MS, omsConfig.getChronicle().getTailPollIntervalMs());
            registrar.addFixedDelayTask(chronicleControlTailReader::pollBatch, Duration.ofMillis(ms));
        };
    }

    private static RollCycle resolveRollCycle(String configured) {
        String key = configured == null ? "" : configured.trim().toUpperCase();
        return switch (key) {
            case "HOURLY" -> LegacyRollCycles.HOURLY;
            case "MINUTELY" -> LegacyRollCycles.MINUTELY;
            case "FAST_DAILY" -> RollCycles.FAST_DAILY;
            case "DEFAULT" -> RollCycles.DEFAULT;
            case "DAILY", "" -> LegacyRollCycles.DAILY;
            default -> LegacyRollCycles.DAILY;
        };
    }
}
