package com.balh.oms.cluster.snapshot;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OmsClusterSnapshotOnShutdownTest {

    @Mock
    OmsAdmissionClusteredService service;

    @Mock
    OmsClusterSnapshotScheduler scheduler;

    @Test
    void takeIfReady_whenNotReady_doesNotTrigger() {
        when(service.isReadyForClusterAdmission()).thenReturn(false);
        assertThat(OmsClusterSnapshotOnShutdown.takeIfReady(service, scheduler, false)).isFalse();
        verify(scheduler, never()).snapshotNow();
    }

    @Test
    void takeIfReady_whenReady_triggers() {
        when(service.isReadyForClusterAdmission()).thenReturn(true);
        when(scheduler.snapshotNow()).thenReturn(true);
        assertThat(OmsClusterSnapshotOnShutdown.takeIfReady(service, scheduler, false)).isTrue();
        verify(scheduler).snapshotNow();
    }
}
