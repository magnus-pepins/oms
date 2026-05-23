package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

class OmsAdmissionReadinessCounterTest {

    private static final long ANY_TIMESTAMP_MS = 1_700_000_000_111L;
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    private OmsAdmissionClusteredService service;
    private Cluster cluster;
    private ClientSession session;

    @BeforeEach
    void setUp() {
        service = new OmsAdmissionClusteredService(new SimpleMeterRegistry());
        session = Mockito.mock(ClientSession.class);
        when(session.offer(any(), anyInt(), anyInt())).thenReturn(1L);

        cluster = Mockito.mock(Cluster.class);
        Aeron aeron = Mockito.mock(Aeron.class);
        when(cluster.aeron()).thenReturn(aeron);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);

        AtomicLong counterValue = new AtomicLong(OmsAdmissionClusteredService.READINESS_VALUE_NOT_READY);
        Counter counter = Mockito.mock(Counter.class);
        when(counter.get()).thenAnswer(inv -> counterValue.get());
        when(counter.id()).thenReturn(42);
        Mockito.doAnswer(inv -> {
                    counterValue.set(inv.getArgument(0, Long.class));
                    return null;
                })
                .when(counter)
                .setOrdered(Mockito.anyLong());
        when(aeron.addCounter(
                        OmsAdmissionClusteredService.READINESS_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.READINESS_COUNTER_LABEL))
                .thenReturn(counter);

        // Phase 3 open-orders counter — onStart unconditionally allocates this; needs a mock or NPE.
        Counter openOrdersCounter = Mockito.mock(Counter.class);
        when(openOrdersCounter.get()).thenReturn(0L);
        when(openOrdersCounter.id()).thenReturn(43);
        when(aeron.addCounter(
                        OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_LABEL))
                .thenReturn(openOrdersCounter);

        // Phase 7 self-healing snapshot-load-failed counter — same NPE class as above.
        Counter snapshotLoadFailedCounter = Mockito.mock(Counter.class);
        when(snapshotLoadFailedCounter.get()).thenReturn(0L);
        when(snapshotLoadFailedCounter.id()).thenReturn(44);
        when(aeron.addCounter(
                        OmsAdmissionClusteredService.SNAPSHOT_LOAD_FAILED_COUNTER_TYPE_ID,
                        OmsAdmissionClusteredService.SNAPSHOT_LOAD_FAILED_COUNTER_LABEL))
                .thenReturn(snapshotLoadFailedCounter);

        ExclusivePublication eventsPub = Mockito.mock(ExclusivePublication.class);
        when(eventsPub.offer(any(), anyInt(), anyInt())).thenReturn(1L);
        when(aeron.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(eventsPub);

        service.onStart(cluster, null);
    }

    @Test
    void onRoleChange_afterReplayWithState_flipsReady() {
        AcceptOrderCommand cmd = sampleAccept(1L, "acct", "idem", UUID.randomUUID());
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, ANY_TIMESTAMP_MS, buffer, 0, written, null);

        assertThat(service.isReadyForClusterAdmission()).isFalse();
        service.onRoleChange(Cluster.Role.FOLLOWER);
        assertThat(service.isReadyForClusterAdmission()).isTrue();
    }

    private static AcceptOrderCommand sampleAccept(
            long correlationId, String accountId, String idemKey, UUID orderId) {
        return new AcceptOrderCommand(
                correlationId,
                orderId,
                0L,
                10_000_000_000L,
                0L,
                0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId,
                idemKey,
                "hash-" + accountId,
                "AAPL",
                null);
    }
}
