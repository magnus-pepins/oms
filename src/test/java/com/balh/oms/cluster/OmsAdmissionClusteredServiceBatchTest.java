package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 4 Tier 2.5 phase D-6: tests for {@link OmsAdmissionClusteredService}'s
 * {@code applyBatchAcceptOrder} dispatch.
 *
 * <p>The contract under test: a {@link BatchAcceptOrderCommand} arriving via
 * {@link OmsAdmissionClusteredService#onSessionMessage} must (1) decode each inner
 * {@link AcceptOrderCommand}, (2) dispatch through the same {@code applyAcceptOrder} path used
 * by the unbatched {@link OmsClusterWireFormat#TYPE_ID_ACCEPT_ORDER}, (3) preserve batch arrival
 * order so replay produces identical egress, and (4) keep the existing
 * {@code (accountId, clientIdempotencyKey)} dedupe semantics unchanged across batched and
 * non-batched arrivals.
 */
class OmsAdmissionClusteredServiceBatchTest {

    private static final long ANY_TIMESTAMP_MS = 1_700_000_000_222L;
    private static final int FRAME_OFFSET = 0;

    private OmsAdmissionClusteredService service;
    private ClientSession session;
    private List<byte[]> capturedEgress;

    @BeforeEach
    void setUp() {
        service = new OmsAdmissionClusteredService();

        capturedEgress = new ArrayList<>();
        session = mock(ClientSession.class);
        when(session.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(this::captureEgress);

        ExclusivePublication eventsPublicationMock = mock(ExclusivePublication.class);
        when(eventsPublicationMock.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenReturn(1L);

        Aeron aeronMock = mock(Aeron.class);
        when(aeronMock.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(eventsPublicationMock);

        Cluster clusterMock = mock(Cluster.class);
        when(clusterMock.role()).thenReturn(Cluster.Role.LEADER);
        when(clusterMock.aeron()).thenReturn(aeronMock);
        service.onStart(clusterMock, /* snapshotImage = */ null);
    }

    private long captureEgress(InvocationOnMock inv) {
        DirectBuffer buf = inv.getArgument(0);
        int off = inv.getArgument(1);
        int len = inv.getArgument(2);
        byte[] copy = new byte[len];
        buf.getBytes(off, copy);
        capturedEgress.add(copy);
        return 1L;
    }

    @Test
    void batchOfFresh_emitsAcceptedEventsInArrivalOrder_andRegistersAllOrders() {
        List<AcceptOrderCommand> inners = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            inners.add(sampleAccept(
                    /* correlationId = */ 100L + i,
                    "acct-" + i,
                    "idem-" + i,
                    new UUID(0L, 1_000L + i)));
        }

        deliverBatch(inners, ANY_TIMESTAMP_MS);

        assertThat(capturedEgress).hasSize(5);
        for (int i = 0; i < 5; i++) {
            OrderAcceptedEvent ev = OrderAcceptedEvent.decode(
                    new UnsafeBuffer(capturedEgress.get(i)), 0);
            assertThat(ev.correlationId()).isEqualTo(100L + i);
            assertThat(ev.duplicate()).isFalse();
            assertThat(ev.orderId()).isEqualTo(inners.get(i).orderId());
            assertThat(ev.acceptedAtMillis()).isEqualTo(ANY_TIMESTAMP_MS);
        }
        assertThat(service.admittedOrderCount()).isEqualTo(5);
    }

    @Test
    void batchWithDuplicateInner_emitsDuplicateBitTrueForReHit() {
        UUID firstOrderId = new UUID(0L, 2_001L);
        UUID secondOrderIdSameKey = new UUID(0L, 2_002L);

        List<AcceptOrderCommand> inners = List.of(
                sampleAccept(1L, "acct", "idem", firstOrderId),
                sampleAccept(2L, "acct", "idem", secondOrderIdSameKey));
        deliverBatch(inners, ANY_TIMESTAMP_MS);

        assertThat(capturedEgress).hasSize(2);
        OrderAcceptedEvent first = OrderAcceptedEvent.decode(
                new UnsafeBuffer(capturedEgress.get(0)), 0);
        OrderAcceptedEvent second = OrderAcceptedEvent.decode(
                new UnsafeBuffer(capturedEgress.get(1)), 0);

        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.orderId()).isEqualTo(firstOrderId);
        assertThat(second.correlationId()).isEqualTo(2L);
        assertThat(service.admittedOrderCount()).isEqualTo(1);
    }

    @Test
    void batchOfOne_isEquivalentToUnbatchedAccept() {
        AcceptOrderCommand inner = sampleAccept(99L, "acct", "idem", new UUID(0L, 3_001L));

        deliverBatch(List.of(inner), ANY_TIMESTAMP_MS);

        assertThat(capturedEgress).hasSize(1);
        OrderAcceptedEvent ev = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        assertThat(ev.correlationId()).isEqualTo(99L);
        assertThat(ev.duplicate()).isFalse();
        assertThat(service.admittedOrderCount()).isEqualTo(1);
    }

    @Test
    void malformedBatch_isIgnoredSilently() {
        // Header says count=1 but no inner-length-prefix follows. The service contract requires
        // silent skip (a throw on replay would break determinism).
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(64);
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, 1);
        int truncatedLen = OmsClusterWireFormat.HEADER_LENGTH + BatchAcceptOrderCommand.COUNT_LENGTH;

        service.onSessionMessage(
                session, ANY_TIMESTAMP_MS, batch, FRAME_OFFSET, truncatedLen, /* header = */ null);

        assertThat(capturedEgress).isEmpty();
        assertThat(service.admittedOrderCount()).isZero();
    }

    @Test
    void unbatchedAcceptThenBatchedDuplicateOfSameKey_emitsDuplicateBit() {
        UUID firstOrderId = new UUID(0L, 4_001L);
        UUID duplicateInBatch = new UUID(0L, 4_002L);

        // First admit via the unbatched path
        deliverUnbatched(sampleAccept(1L, "acct", "idem", firstOrderId), ANY_TIMESTAMP_MS);
        // Then a batch whose inner targets the same idempotency key
        deliverBatch(
                List.of(sampleAccept(2L, "acct", "idem", duplicateInBatch)),
                ANY_TIMESTAMP_MS + 1);

        assertThat(capturedEgress).hasSize(2);
        OrderAcceptedEvent first = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        OrderAcceptedEvent second = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(1)), 0);
        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.orderId()).isEqualTo(firstOrderId);
        assertThat(service.admittedOrderCount()).isEqualTo(1);
    }

    private void deliverBatch(List<AcceptOrderCommand> inners, long clusterTimestampMillis) {
        ExpandableArrayBuffer perCmd = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        ExpandableArrayBuffer batch = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_BATCH_COMMAND_BYTES);
        int p = BatchAcceptOrderCommand.firstInnerOffset(FRAME_OFFSET);
        for (AcceptOrderCommand c : inners) {
            int innerLen = c.encode(perCmd, 0);
            p = BatchAcceptOrderCommand.writeInner(batch, p, perCmd, 0, innerLen);
        }
        BatchAcceptOrderCommand.writeHeader(batch, FRAME_OFFSET, inners.size());
        int totalBytes = BatchAcceptOrderCommand.totalEncodedLength(p);

        service.onSessionMessage(
                session, clusterTimestampMillis, batch, FRAME_OFFSET, totalBytes, /* header = */ null);
    }

    private void deliverUnbatched(AcceptOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(
                session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private static AcceptOrderCommand sampleAccept(
            long correlationId, String accountId, String idemKey, UUID orderId) {
        return new AcceptOrderCommand(
                correlationId,
                orderId,
                /* clientTimestampNanos = */ 0L,
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                accountId,
                idemKey,
                "hash-" + accountId,
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }
}
