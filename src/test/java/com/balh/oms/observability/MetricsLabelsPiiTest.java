package com.balh.oms.observability;

import com.balh.oms.AbstractPostgresIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the PII policy: no Micrometer meter may carry a label whose key is one
 * of {@link MetricsConfig#FORBIDDEN_LABELS}.
 *
 * <p>Combined with the runtime {@code MeterFilter} (which strips offenders),
 * this test catches developers adding a forbidden tag in code review.
 */
class MetricsLabelsPiiTest extends AbstractPostgresIntegrationTest {

    @Autowired MeterRegistry registry;

    @Test
    void noMeterCarriesAccountIdLikeLabels() {
        var offenders = registry.getMeters().stream()
                .flatMap(m -> m.getId().getTags().stream())
                .filter(t -> MetricsConfig.FORBIDDEN_LABELS.contains(t.getKey().toLowerCase()))
                .toList();
        assertThat(offenders)
                .as("Micrometer must not expose raw PII labels (see oms/docs/pii-policy.md)")
                .isEmpty();
    }
}
