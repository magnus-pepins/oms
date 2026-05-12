package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deterministic state-machine tests for {@link OmsAdmissionClusteredService}.
 *
 * <p>Mocks {@link ClientSession}, {@link ExclusivePublication}, and {@link Image}
 * so we can capture egress events and snapshot bytes without a real Aeron
 * cluster. Validates the invariants ADR 0001 commits to:
 * <ul>
 *     <li>Fresh accept emits a non-duplicate {@link OrderAcceptedEvent} and
 *         registers state under both indexes (idempotency + orderId).</li>
 *     <li>Same {@code (accountId, clientIdempotencyKey)} replayed re-emits with
 *         {@code duplicate=true} and does not double-register state.</li>
 *     <li>Snapshot encode -> decode against a fresh service instance restores
 *         identical state (this is what gives us deterministic replay across
 *         leader / follower / restart).</li>
 * </ul>
 *
 * <p>Cluster timestamps are supplied to the apply path; the state machine must
 * not call {@code Instant.now()} or similar (per ADR 0001 § Discipline).
 */
class OmsAdmissionClusteredServiceTest {

    /** Arbitrary cluster-supplied timestamp used in tests. */
    private static final long ANY_TIMESTAMP_NS = 1_700_000_000_111_222L;

    /**
     * Encoded buffer capacity for the per-test command. Admission commands
     * are bounded by {@link OmsClusterWireFormat#MAX_COMMAND_BYTES}.
     */
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    private OmsAdmissionClusteredService service;
    private ClientSession session;
    private List<byte[]> capturedEgress;
    private List<byte[]> capturedAdmittedEvents;
    private Cluster clusterMock;
    private ExclusivePublication eventsPublicationMock;

    @BeforeEach
    void setUp() {
        service = new OmsAdmissionClusteredService();

        capturedEgress = new ArrayList<>();
        session = mock(ClientSession.class);
        when(session.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(this::captureEgress);

        capturedAdmittedEvents = new ArrayList<>();
        eventsPublicationMock = mock(ExclusivePublication.class);
        when(eventsPublicationMock.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(this::captureAdmitted);

        Aeron aeronMock = mock(Aeron.class);
        when(aeronMock.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(eventsPublicationMock);

        clusterMock = mock(Cluster.class);
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

    private long captureAdmitted(InvocationOnMock inv) {
        DirectBuffer buf = inv.getArgument(0);
        int off = inv.getArgument(1);
        int len = inv.getArgument(2);
        byte[] copy = new byte[len];
        buf.getBytes(off, copy);
        capturedAdmittedEvents.add(copy);
        return 1L;
    }

    @Test
    void freshAccept_emitsNonDuplicateAcceptedEvent_andRegistersOrder() {
        AcceptOrderCommand cmd = sampleAccept(
                /* correlationId = */ 1L,
                "acct-A",
                "idem-1",
                UUID.fromString("00000000-0000-4000-8000-000000000001"));

        deliverCommand(cmd, ANY_TIMESTAMP_NS);

        assertThat(capturedEgress).hasSize(1);
        OrderAcceptedEvent ev = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        assertThat(ev.correlationId()).isEqualTo(1L);
        assertThat(ev.duplicate()).isFalse();
        assertThat(ev.orderId()).isEqualTo(cmd.orderId());
        assertThat(ev.acceptedAtNanos()).isEqualTo(ANY_TIMESTAMP_NS);

        assertThat(service.admittedOrderCount()).isEqualTo(1);
        assertThat(service.lookupByIdempotency("acct-A", "idem-1")).isNotNull();
        assertThat(service.lookupByOrderId(cmd.orderId())).isNotNull();
    }

    @Test
    void idempotentReHit_emitsDuplicateBitTrue_andDoesNotDoubleRegister() {
        UUID firstOrderId = UUID.fromString("00000000-0000-4000-8000-000000000010");
        UUID secondOrderId = UUID.fromString("00000000-0000-4000-8000-000000000011");

        deliverCommand(sampleAccept(1L, "acct", "idem", firstOrderId), ANY_TIMESTAMP_NS);
        deliverCommand(sampleAccept(2L, "acct", "idem", secondOrderId), ANY_TIMESTAMP_NS + 1);

        assertThat(capturedEgress).hasSize(2);
        OrderAcceptedEvent first = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        OrderAcceptedEvent second = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(1)), 0);

        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        // The second event echoes the FIRST registered order id, not the duplicate caller's id.
        assertThat(second.orderId()).isEqualTo(firstOrderId);
        assertThat(second.correlationId()).isEqualTo(2L);

        assertThat(service.admittedOrderCount()).isEqualTo(1);
    }

    @Test
    void differentIdempotencyKeys_areIndependentOrders() {
        deliverCommand(
                sampleAccept(1L, "acct", "idem-A", UUID.fromString("00000000-0000-4000-8000-000000000020")),
                ANY_TIMESTAMP_NS);
        deliverCommand(
                sampleAccept(2L, "acct", "idem-B", UUID.fromString("00000000-0000-4000-8000-000000000021")),
                ANY_TIMESTAMP_NS + 1);

        assertThat(capturedEgress).hasSize(2);
        OrderAcceptedEvent first = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        OrderAcceptedEvent second = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(1)), 0);
        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isFalse();
        assertThat(service.admittedOrderCount()).isEqualTo(2);
    }

    @Test
    void freshAccept_emitsOrderAdmittedEvent_toEventsPublication() {
        AcceptOrderCommand cmd = sampleAccept(
                /* correlationId = */ 1L,
                "acct-A",
                "idem-1",
                UUID.fromString("00000000-0000-4000-8000-000000000040"));

        deliverCommand(cmd, ANY_TIMESTAMP_NS);

        assertThat(capturedAdmittedEvents).hasSize(1);
        OrderAdmittedEvent admitted = OrderAdmittedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)),
                0,
                capturedAdmittedEvents.get(0).length);
        assertThat(admitted.orderId()).isEqualTo(cmd.orderId());
        assertThat(admitted.accountId()).isEqualTo(cmd.accountId());
        assertThat(admitted.clientIdempotencyKey()).isEqualTo(cmd.clientIdempotencyKey());
        assertThat(admitted.accountIdHash()).isEqualTo(cmd.accountIdHash());
        assertThat(admitted.instrumentSymbol()).isEqualTo(cmd.instrumentSymbol());
        assertThat(admitted.quantityScaled()).isEqualTo(cmd.quantityScaled());
        assertThat(admitted.limitPriceScaledOrZero()).isEqualTo(cmd.limitPriceScaledOrZero());
        assertThat(admitted.shardId()).isEqualTo(cmd.shardId());
        assertThat(admitted.side()).isEqualTo(cmd.side());
        assertThat(admitted.timeInForceCode()).isEqualTo(cmd.timeInForceCode());
        assertThat(admitted.acceptedAtNanos()).isEqualTo(ANY_TIMESTAMP_NS);
        assertThat(admitted.version()).isZero();
    }

    @Test
    void idempotentReHit_doesNotReEmitAdmittedEvent() {
        UUID firstOrderId = UUID.fromString("00000000-0000-4000-8000-000000000050");
        UUID secondOrderId = UUID.fromString("00000000-0000-4000-8000-000000000051");

        deliverCommand(sampleAccept(1L, "acct", "idem", firstOrderId), ANY_TIMESTAMP_NS);
        assertThat(capturedAdmittedEvents).hasSize(1);

        deliverCommand(sampleAccept(2L, "acct", "idem", secondOrderId), ANY_TIMESTAMP_NS + 1);
        // Per-session egress emitted twice (the duplicate path tells the second caller); the side
        // publication only saw the first emission — the projector relies on this to avoid duplicate rows.
        assertThat(capturedEgress).hasSize(2);
        assertThat(capturedAdmittedEvents).hasSize(1);
    }

    @Test
    void onSessionMessage_malformedPayload_isIgnoredSilently() {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.HEADER_LENGTH);
        // Length below header is a malformed command and must be a no-op (not an exception).
        service.onSessionMessage(session, ANY_TIMESTAMP_NS, buffer, 0, OmsClusterWireFormat.HEADER_LENGTH - 1, null);

        assertThat(capturedEgress).isEmpty();
        assertThat(service.admittedOrderCount()).isZero();
    }

    @Test
    void snapshot_savedAndReloaded_restoresEntireIdempotencyState() {
        UUID orderA = UUID.fromString("00000000-0000-4000-8000-000000000030");
        UUID orderB = UUID.fromString("00000000-0000-4000-8000-000000000031");
        deliverCommand(sampleAccept(1L, "acctA", "idem-A", orderA), ANY_TIMESTAMP_NS);
        deliverCommand(sampleAccept(2L, "acctB", "idem-B", orderB), ANY_TIMESTAMP_NS + 1);

        byte[] snapshotBytes = takeSnapshotBytes(service);

        OmsAdmissionClusteredService restored = new OmsAdmissionClusteredService();
        Cluster mockCluster2 = mock(Cluster.class);
        when(mockCluster2.role()).thenReturn(Cluster.Role.FOLLOWER);
        Aeron aeronMock2 = mock(Aeron.class);
        when(aeronMock2.addExclusivePublication(
                        OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID))
                .thenReturn(mock(ExclusivePublication.class));
        when(mockCluster2.aeron()).thenReturn(aeronMock2);
        Image snapshotImage = mockSnapshotImage(snapshotBytes);
        restored.onStart(mockCluster2, snapshotImage);

        assertThat(restored.admittedOrderCount()).isEqualTo(2);
        assertThat(restored.lookupByIdempotency("acctA", "idem-A")).isNotNull();
        assertThat(restored.lookupByIdempotency("acctB", "idem-B")).isNotNull();
        assertThat(restored.lookupByOrderId(orderA)).isNotNull();
        assertThat(restored.lookupByOrderId(orderB)).isNotNull();
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private void deliverCommand(AcceptOrderCommand cmd, long clusterTimestampNs) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampNs, buffer, 0, written, /* header = */ null);
    }

    private static AcceptOrderCommand sampleAccept(long correlationId, String accountId, String idemKey, UUID orderId) {
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

    /**
     * Capture the snapshot bytes the service writes via
     * {@link OmsAdmissionClusteredService#onTakeSnapshot}. Real Aeron snapshots
     * may fragment across multiple {@code offer} calls; the admission state
     * machine writes one fragment today, so this helper concatenates whatever
     * the service wrote into a single buffer.
     */
    private static byte[] takeSnapshotBytes(OmsAdmissionClusteredService svc) {
        ExpandableArrayBuffer accumulator = new ExpandableArrayBuffer(1024);
        int[] written = {0};
        ExclusivePublication snapshotPub = mock(ExclusivePublication.class);
        when(snapshotPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(inv -> {
            DirectBuffer src = inv.getArgument(0);
            int off = inv.getArgument(1);
            int len = inv.getArgument(2);
            accumulator.putBytes(written[0], src, off, len);
            written[0] += len;
            return 1L;
        });
        svc.onTakeSnapshot(snapshotPub);
        byte[] copy = new byte[written[0]];
        accumulator.getBytes(0, copy);
        return copy;
    }

    /**
     * Build a mocked {@link Image} that delivers {@code snapshotBytes} as a
     * single fragment then reports end-of-stream. Suffices for
     * {@link OmsAdmissionClusteredService}'s snapshot loader (one fragment per
     * snapshot today).
     */
    private static Image mockSnapshotImage(byte[] snapshotBytes) {
        Image image = mock(Image.class);
        AtomicBoolean delivered = new AtomicBoolean(false);
        when(image.isEndOfStream()).thenAnswer(inv -> delivered.get());
        when(image.poll(any(FragmentHandler.class), anyInt())).thenAnswer(inv -> {
            if (delivered.get()) {
                return 0;
            }
            FragmentHandler handler = inv.getArgument(0);
            UnsafeBuffer buffer = new UnsafeBuffer(snapshotBytes);
            handler.onFragment(buffer, 0, snapshotBytes.length, /* header = */ null);
            delivered.set(true);
            return 1;
        });
        return image;
    }
}
