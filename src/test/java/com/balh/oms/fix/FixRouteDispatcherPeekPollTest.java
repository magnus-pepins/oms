package com.balh.oms.fix;

import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FixRouteDispatcherPeekPollTest {

    @Test
    void peekPending_doesNotRemoveHead() {
        LinkedBlockingQueue<FixOutboundWireJob> q = new LinkedBlockingQueue<>(10);
        IngressToFixNosLatencyRecorder rec = mock(IngressToFixNosLatencyRecorder.class);
        FixRouteDispatcher d = new FixRouteDispatcher(q, rec);
        UUID id = UUID.randomUUID();
        d.enqueueWorkingOrder(id);
        assertThat(d.peekPendingOrNull()).isEqualTo(new FixOutboundWireJob.NosOrder(id));
        assertThat(d.peekPendingOrNull()).isEqualTo(new FixOutboundWireJob.NosOrder(id));
        assertThat(q.size()).isEqualTo(1);
        assertThat(d.pollPendingOrNull()).isEqualTo(new FixOutboundWireJob.NosOrder(id));
        assertThat(d.peekPendingOrNull()).isNull();
    }
}
