package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/** Shared Aeron mocks for {@link OmsAdmissionClusteredService} unit tests. */
final class OmsAdmissionClusteredServiceTestFixtures {

    private OmsAdmissionClusteredServiceTestFixtures() {}

    static void wireClusterAeronMocks(Aeron aeronMock, ExclusivePublication eventsPublicationMock) {
        when(aeronMock.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(eventsPublicationMock);
        io.aeron.Counter readinessCounter = Mockito.mock(io.aeron.Counter.class);
        when(readinessCounter.get()).thenReturn(OmsAdmissionClusteredService.READINESS_VALUE_NOT_READY);
        when(readinessCounter.id()).thenReturn(1);
        when(aeronMock.addCounter(
                        OmsAdmissionClusteredService.READINESS_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.READINESS_COUNTER_LABEL))
                .thenReturn(readinessCounter);
        io.aeron.Counter openOrdersCounter = Mockito.mock(io.aeron.Counter.class);
        when(openOrdersCounter.get()).thenReturn(0L);
        when(openOrdersCounter.id()).thenReturn(2);
        when(aeronMock.addCounter(
                        OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_LABEL))
                .thenReturn(openOrdersCounter);
        // Phase 7 self-healing snapshot-load-failed counter — must be wired here so existing tests
        // don't NPE on the cluster.aeron().addCounter(...) call inside onStart.
        io.aeron.Counter snapshotLoadFailedCounter = Mockito.mock(io.aeron.Counter.class);
        when(snapshotLoadFailedCounter.get()).thenReturn(0L);
        when(snapshotLoadFailedCounter.id()).thenReturn(3);
        when(aeronMock.addCounter(
                        OmsAdmissionClusteredService.SNAPSHOT_LOAD_FAILED_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.SNAPSHOT_LOAD_FAILED_COUNTER_LABEL))
                .thenReturn(snapshotLoadFailedCounter);
    }
}
