package com.balh.oms.observability;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

/**
 * Locks down Micrometer label cardinality to keep raw PII out of metrics.
 *
 * <p>If a meter ever ships with a label name in {@link #FORBIDDEN_LABELS}, this
 * filter removes it. Combined with the PII lint test, we get defence in depth
 * against accidental leakage.
 */
@Configuration
public class MetricsConfig {

    public static final Set<String> FORBIDDEN_LABELS = Set.of(
            "account_id",
            "accountid",
            "user_id",
            "userid",
            "email"
    );

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> piiLabelGuard() {
        return registry -> registry.config().meterFilter(MeterFilter.deny(id ->
                id.getTags().stream().anyMatch(t ->
                        FORBIDDEN_LABELS.contains(t.getKey().toLowerCase()))));
    }

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> defaultTags(OmsConfig config) {
        return registry -> registry.config().commonTags(
                "shard_id", Integer.toString(config.getShard().getId())
        );
    }
}
