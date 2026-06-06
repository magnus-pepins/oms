package com.balh.oms.ingress.readiness;

import com.balh.oms.cluster.disk.AeronDiskPressureLevel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReadinessSnapshotDiskPressureTest {

    @Test
    void disk_reject_overrides_cluster_ready() {
        ReadinessSnapshot snapshot = ReadinessSnapshot.observedWithDiskPressure(
                1L,
                10,
                System.currentTimeMillis(),
                1L,
                AeronDiskPressureLevel.REJECT,
                2L,
                11);
        assertThat(snapshot.status()).isEqualTo(ReadinessSnapshot.Status.DISK_PRESSURE);
        assertThat(snapshot.isReady()).isFalse();
    }

    @Test
    void cluster_ready_with_disk_ok() {
        ReadinessSnapshot snapshot = ReadinessSnapshot.observedWithDiskPressure(
                1L,
                10,
                System.currentTimeMillis(),
                1L,
                AeronDiskPressureLevel.OK,
                0L,
                11);
        assertThat(snapshot.status()).isEqualTo(ReadinessSnapshot.Status.READY);
        assertThat(snapshot.isReady()).isTrue();
    }
}
