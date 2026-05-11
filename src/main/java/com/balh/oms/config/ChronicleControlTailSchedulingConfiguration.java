package com.balh.oms.config;

import com.balh.oms.chronicle.ChronicleControlTailReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Duration;

/**
 * Wires {@link ChronicleControlTailReader#pollBatch()} on a fixed delay when {@code oms.chronicle.tail-driver}
 * is {@code scheduled}. When {@code dedicated}, no scheduled task is registered (see dedicated thread in the reader).
 */
@Configuration(proxyBeanMethods = false)
@Profile("!test")
@ConditionalOnBean(ChronicleControlTailReader.class)
@ConditionalOnProperty(prefix = "oms.chronicle", name = "tail-driver", havingValue = "scheduled", matchIfMissing = true)
public class ChronicleControlTailSchedulingConfiguration implements SchedulingConfigurer {

    private static final long MIN_TAIL_POLL_INTERVAL_MS = 1L;

    private final ChronicleControlTailReader chronicleControlTailReader;
    private final OmsConfig omsConfig;

    public ChronicleControlTailSchedulingConfiguration(
            ChronicleControlTailReader chronicleControlTailReader,
            OmsConfig omsConfig) {
        this.chronicleControlTailReader = chronicleControlTailReader;
        this.omsConfig = omsConfig;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        long ms = Math.max(MIN_TAIL_POLL_INTERVAL_MS, omsConfig.getChronicle().getTailPollIntervalMs());
        registrar.addFixedDelayTask(chronicleControlTailReader::pollBatch, Duration.ofMillis(ms));
    }
}
