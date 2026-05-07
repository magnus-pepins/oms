package com.balh.oms.config;

import com.balh.oms.chronicle.ChronicleControlJournal;
import com.balh.oms.chronicle.ChronicleControlTailReader;
import com.balh.oms.tailer.ControlTailer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.nio.file.Path;

/**
 * Registers the shared {@link ChronicleQueue} plus append-only journal and
 * scheduled tail reader. Disabled in the {@code test} profile (tests use
 * {@link com.balh.oms.chronicle.NoOpControlJournal} instead).
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
    public ChronicleControlTailReader chronicleControlTailReader(
            ChronicleQueue controlChronicleQueue,
            ControlTailer controlTailer,
            ObjectMapper objectMapper,
            OmsConfig config,
            MeterRegistry meterRegistry) {
        return new ChronicleControlTailReader(
                controlChronicleQueue, controlTailer, objectMapper, config, meterRegistry);
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
