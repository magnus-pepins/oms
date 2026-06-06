package com.balh.oms.cluster.disk;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class AeronDiskPressurePolicyTest {

    private static final long GIB = 1024L * 1024L * 1024L;

    @Test
    void evaluate_ok_when_plenty_of_space() {
        AeronDiskPressurePolicy policy = policyWithDefaults();
        assertThat(policy.evaluate(new AeronDiskSpaceSample(50L * GIB, 100L * GIB)))
                .isEqualTo(AeronDiskPressureLevel.OK);
    }

    @Test
    void evaluate_reject_when_bytes_below_threshold() {
        AeronDiskPressurePolicy policy = policyWithDefaults();
        // Total large enough that ratio alone does not escalate past REJECT.
        assertThat(policy.evaluate(new AeronDiskSpaceSample(4L * GIB, 60L * GIB)))
                .isEqualTo(AeronDiskPressureLevel.REJECT);
    }

    @Test
    void evaluate_critical_when_bytes_very_low() {
        AeronDiskPressurePolicy policy = policyWithDefaults();
        assertThat(policy.evaluate(new AeronDiskSpaceSample(GIB, 100L * GIB)))
                .isEqualTo(AeronDiskPressureLevel.CRITICAL);
    }

    @Test
    void evaluate_uses_more_severe_of_bytes_and_ratio() {
        AeronDiskPressurePolicy policy = policyWithDefaults();
        // Ratio 8% triggers REJECT even though absolute bytes are above the 5 GiB reject floor.
        assertThat(policy.evaluate(new AeronDiskSpaceSample(20L * GIB, 250L * GIB)))
                .isEqualTo(AeronDiskPressureLevel.REJECT);
        // Ratio 4% triggers CRITICAL.
        assertThat(policy.evaluate(new AeronDiskSpaceSample(20L * GIB, 500L * GIB)))
                .isEqualTo(AeronDiskPressureLevel.CRITICAL);
    }

    @Test
    void diskPressureLevel_blocks_writes_from_reject_upward() {
        assertThat(AeronDiskPressureLevel.OK.blocksWrites()).isFalse();
        assertThat(AeronDiskPressureLevel.WARN.blocksWrites()).isFalse();
        assertThat(AeronDiskPressureLevel.REJECT.blocksWrites()).isTrue();
        assertThat(AeronDiskPressureLevel.CRITICAL.blocksWrites()).isTrue();
    }

    private static AeronDiskPressurePolicy policyWithDefaults() {
        return new AeronDiskPressurePolicy(
                true,
                Path.of("build/aeron-cluster"),
                10L * GIB,
                5L * GIB,
                2L * GIB,
                0.15d,
                0.10d,
                0.05d,
                5_000L);
    }
}
