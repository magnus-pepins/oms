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
    }
}
