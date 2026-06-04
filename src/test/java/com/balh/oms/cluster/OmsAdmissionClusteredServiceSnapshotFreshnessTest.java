package com.balh.oms.cluster;

import io.aeron.ExclusivePublication;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 4 slice 4h — focused tests for the new {@code oms.cluster.snapshot.age_seconds} gauge.
 *
 * <p>Boots a {@link OmsAdmissionClusteredService} with a {@link SimpleMeterRegistry} (no Aeron, no
 * Spring), asserts the gauge is registered with the right meta (so dashboards / alerts find it on
 * a freshly booted cluster), and asserts a successful {@code onTakeSnapshot} resets the underlying
 * timestamp. Covers the freshness-alert wiring documented in {@code oms/docs/cluster-slo.md}.
 */
class OmsAdmissionClusteredServiceSnapshotFreshnessTest {

    private static final String GAUGE_NAME = "oms.cluster.snapshot.age_seconds";

    @Test
    void snapshotAgeGauge_isRegisteredWithExpectedMeta() {
        MeterRegistry registry = new SimpleMeterRegistry();
        new OmsAdmissionClusteredService(registry);

        Gauge gauge = registry.find(GAUGE_NAME).gauge();
        assertThat(gauge).as("gauge must be pre-registered so the slice 4h alert wires to a present series").isNotNull();
        assertThat(gauge.getId().getBaseUnit()).isEqualTo("seconds");
        assertThat(gauge.getId().getDescription()).contains("snapshot");
    }

    @Test
    void snapshotAgeGauge_freshlyConstructedService_reportsNearZeroSeconds() {
        MeterRegistry registry = new SimpleMeterRegistry();
        new OmsAdmissionClusteredService(registry);

        // Construct-time init (see class javadoc on lastSnapshotWriteEpochMs): on a fresh boot the
        // gauge starts near zero rather than at 1970 epoch — otherwise the alert would fire
        // immediately on boot before the operator / cron has had a chance to snapshot.
        double age = registry.get(GAUGE_NAME).gauge().value();
        assertThat(age).isGreaterThanOrEqualTo(0.0);
        // Test runtime is sub-second; allow generous slack for a slow CI runner.
        assertThat(age).isLessThan(5.0);
    }

    @Test
    void snapshotAgeGauge_resetsAfterSuccessfulOnTakeSnapshot() throws Exception {
        MeterRegistry registry = new SimpleMeterRegistry();
        OmsAdmissionClusteredService service = new OmsAdmissionClusteredService(registry);

        // Wait long enough that the gauge would observably climb if onTakeSnapshot didn't reset it.
        Thread.sleep(50L);
        double ageBefore = registry.get(GAUGE_NAME).gauge().value();
        assertThat(ageBefore).isGreaterThan(0.0);

        ExclusivePublication snapshotPub = mock(ExclusivePublication.class);
        when(snapshotPub.maxMessageLength()).thenReturn(8 * 1024 * 1024);
        ExpandableArrayBuffer accumulator = new ExpandableArrayBuffer(256);
        int[] written = {0};
        when(snapshotPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(inv -> {
            DirectBuffer src = inv.getArgument(0);
            int off = inv.getArgument(1);
            int len = inv.getArgument(2);
            accumulator.putBytes(written[0], src, off, len);
            written[0] += len;
            return 1L;
        });
        service.onTakeSnapshot(snapshotPub);

        double ageAfter = registry.get(GAUGE_NAME).gauge().value();
        // After a successful onTakeSnapshot the timestamp resets. The gauge value reflects the
        // wall-clock distance between the reset and this read — trivially small in a unit test
        // but always less than the pre-snapshot reading.
        assertThat(ageAfter).isLessThan(ageBefore);
        assertThat(ageAfter).isLessThan(1.0);
    }
}
