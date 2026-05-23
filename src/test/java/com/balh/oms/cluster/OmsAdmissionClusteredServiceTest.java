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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

    /**
     * Arbitrary cluster-supplied timestamp used in tests. The Aeron Cluster default
     * {@link io.aeron.cluster.ConsensusModule.Context#timeUnit()} is
     * {@link java.util.concurrent.TimeUnit#MILLISECONDS}, so this represents epoch-millis. Phase 4j
     * renamed the field from {@code acceptedAtNanos} to {@code acceptedAtMillis} to match.
     */
    private static final long ANY_TIMESTAMP_MS = 1_700_000_000_111L;

    /**
     * Encoded buffer capacity for the per-test command. Admission commands
     * are bounded by {@link OmsClusterWireFormat#MAX_COMMAND_BYTES}.
     */
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    private SimpleMeterRegistry meterRegistry;
    private OmsAdmissionClusteredService service;
    private ClientSession session;
    private List<byte[]> capturedEgress;
    private List<byte[]> capturedAdmittedEvents;
    private Cluster clusterMock;
    private ExclusivePublication eventsPublicationMock;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new OmsAdmissionClusteredService(meterRegistry);

        capturedEgress = new ArrayList<>();
        session = mock(ClientSession.class);
        when(session.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(this::captureEgress);

        capturedAdmittedEvents = new ArrayList<>();
        eventsPublicationMock = mock(ExclusivePublication.class);
        when(eventsPublicationMock.offer(any(DirectBuffer.class), anyInt(), anyInt()))
                .thenAnswer(this::captureAdmitted);

        Aeron aeronMock = mock(Aeron.class);
        OmsAdmissionClusteredServiceTestFixtures.wireClusterAeronMocks(aeronMock, eventsPublicationMock);

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

        deliverCommand(cmd, ANY_TIMESTAMP_MS);

        assertThat(capturedEgress).hasSize(1);
        OrderAcceptedEvent ev = OrderAcceptedEvent.decode(new UnsafeBuffer(capturedEgress.get(0)), 0);
        assertThat(ev.correlationId()).isEqualTo(1L);
        assertThat(ev.duplicate()).isFalse();
        assertThat(ev.orderId()).isEqualTo(cmd.orderId());
        assertThat(ev.acceptedAtMillis()).isEqualTo(ANY_TIMESTAMP_MS);

        assertThat(service.admittedOrderCount()).isEqualTo(1);
        assertThat(service.lookupByIdempotency("acct-A", "idem-1")).isNotNull();
        assertThat(service.lookupByOrderId(cmd.orderId())).isNotNull();
    }

    @Test
    void idempotentReHit_emitsDuplicateBitTrue_andDoesNotDoubleRegister() {
        UUID firstOrderId = UUID.fromString("00000000-0000-4000-8000-000000000010");
        UUID secondOrderId = UUID.fromString("00000000-0000-4000-8000-000000000011");

        deliverCommand(sampleAccept(1L, "acct", "idem", firstOrderId), ANY_TIMESTAMP_MS);
        deliverCommand(sampleAccept(2L, "acct", "idem", secondOrderId), ANY_TIMESTAMP_MS + 1);

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
                ANY_TIMESTAMP_MS);
        deliverCommand(
                sampleAccept(2L, "acct", "idem-B", UUID.fromString("00000000-0000-4000-8000-000000000021")),
                ANY_TIMESTAMP_MS + 1);

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

        deliverCommand(cmd, ANY_TIMESTAMP_MS);

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
        assertThat(admitted.acceptedAtMillis()).isEqualTo(ANY_TIMESTAMP_MS);
        assertThat(admitted.version()).isZero();
    }

    @Test
    void idempotentReHit_doesNotReEmitAdmittedEvent() {
        UUID firstOrderId = UUID.fromString("00000000-0000-4000-8000-000000000050");
        UUID secondOrderId = UUID.fromString("00000000-0000-4000-8000-000000000051");

        deliverCommand(sampleAccept(1L, "acct", "idem", firstOrderId), ANY_TIMESTAMP_MS);
        assertThat(capturedAdmittedEvents).hasSize(1);

        deliverCommand(sampleAccept(2L, "acct", "idem", secondOrderId), ANY_TIMESTAMP_MS + 1);
        // Per-session egress emitted twice (the duplicate path tells the second caller); the side
        // publication only saw the first emission — the projector relies on this to avoid duplicate rows.
        assertThat(capturedEgress).hasSize(2);
        assertThat(capturedAdmittedEvents).hasSize(1);
    }

    @Test
    void onSessionMessage_malformedPayload_isIgnoredSilently() {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.HEADER_LENGTH);
        // Length below header is a malformed command and must be a no-op (not an exception).
        service.onSessionMessage(session, ANY_TIMESTAMP_MS, buffer, 0, OmsClusterWireFormat.HEADER_LENGTH - 1, null);

        assertThat(capturedEgress).isEmpty();
        assertThat(service.admittedOrderCount()).isZero();
    }

    @Test
    void snapshot_savedAndReloaded_restoresEntireIdempotencyState() {
        UUID orderA = UUID.fromString("00000000-0000-4000-8000-000000000030");
        UUID orderB = UUID.fromString("00000000-0000-4000-8000-000000000031");
        deliverCommand(sampleAccept(1L, "acctA", "idem-A", orderA), ANY_TIMESTAMP_MS);
        deliverCommand(sampleAccept(2L, "acctB", "idem-B", orderB), ANY_TIMESTAMP_MS + 1);

        byte[] snapshotBytes = takeSnapshotBytes(service);

        OmsAdmissionClusteredService restored = newServiceFromSnapshot(snapshotBytes);

        assertThat(restored.admittedOrderCount()).isEqualTo(2);
        assertThat(restored.lookupByIdempotency("acctA", "idem-A")).isNotNull();
        assertThat(restored.lookupByIdempotency("acctB", "idem-B")).isNotNull();
        assertThat(restored.lookupByOrderId(orderA)).isNotNull();
        assertThat(restored.lookupByOrderId(orderB)).isNotNull();
    }

    // ------------------------------------------------------------------------
    // Slice 3c: ApplyExecutionReportCommand state machine
    // ------------------------------------------------------------------------

    @Test
    void admittedOrder_startsAtVersion0_workingStatus_zeroCumQty() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000100");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.version()).isZero();
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_WORKING);
        assertThat(o.cumQtyScaled()).isZero();
    }

    @Test
    void applyTrade_partialFill_emitsExecutionAppliedEvent_andUpdatesState() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000110");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        // Discard the OrderAdmitted event so we can assert on ExecutionApplied alone.
        capturedAdmittedEvents.clear();

        ApplyExecutionReportCommand cmd = sampleTrade(
                orderId,
                /* lastQtyScaled = */ 4_000_000_000L,
                /* lastPxScaled = */ 100_000_000L,
                "EXEC-PARTIAL-1");
        deliverCommand(cmd, ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).hasSize(1);
        ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)), 0, capturedAdmittedEvents.get(0).length);
        assertThat(ev.orderId()).isEqualTo(orderId);
        assertThat(ev.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_TRADE);
        assertThat(ev.newStatusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_PARTIALLY_FILLED);
        assertThat(ev.newCumQtyScaled()).isEqualTo(4_000_000_000L);
        assertThat(ev.newVersion()).isEqualTo(1);
        assertThat(ev.lastQtyScaled()).isEqualTo(4_000_000_000L);
        assertThat(ev.lastPxScaled()).isEqualTo(100_000_000L);
        assertThat(ev.venueExecRef()).isEqualTo("EXEC-PARTIAL-1");
        assertThat(ev.accountId()).isEqualTo("acct");
        assertThat(ev.appliedAtMillis()).isEqualTo(ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_PARTIALLY_FILLED);
        assertThat(o.cumQtyScaled()).isEqualTo(4_000_000_000L);
        assertThat(o.version()).isEqualTo(1);
    }

    @Test
    void applyTrade_fullFillFromPartial_transitionsToFilled() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000111");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(
                sampleTrade(orderId, /* lastQtyScaled = */ 4_000_000_000L, 100_000_000L, "EXEC-1"),
                ANY_TIMESTAMP_MS + 1);
        deliverCommand(
                sampleTrade(orderId, /* lastQtyScaled = */ 6_000_000_000L, 100_000_000L, "EXEC-2"),
                ANY_TIMESTAMP_MS + 2);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_FILLED);
        assertThat(o.cumQtyScaled()).isEqualTo(10_000_000_000L);
        assertThat(o.version()).isEqualTo(2);
        // Three events total: one OrderAdmitted, two ExecutionApplied.
        assertThat(capturedAdmittedEvents).hasSize(3);
    }

    @Test
    void applyTrade_overFill_isSilentlyDropped() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000112");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        // Order quantity is 10_000_000_000 (sampleAccept default); a 12B fill exceeds the order qty
        // and must not mutate state or emit an event.
        deliverCommand(
                sampleTrade(orderId, /* lastQtyScaled = */ 12_000_000_000L, 100_000_000L, "EXEC-OVER"),
                ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).isEmpty();
        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_WORKING);
        assertThat(o.version()).isZero();
        assertThat(o.cumQtyScaled()).isZero();
    }

    @Test
    void applyCancel_workingOrder_transitionsToCancelled() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000120");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        deliverCommand(sampleCancel(orderId, "EXEC-CXL-1"), ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).hasSize(1);
        ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)), 0, capturedAdmittedEvents.get(0).length);
        assertThat(ev.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL);
        assertThat(ev.newStatusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_CANCELLED);
        assertThat(ev.newCumQtyScaled()).isZero();
        assertThat(ev.newVersion()).isEqualTo(1);
    }

    @Test
    void applyVenueReject_workingOrder_transitionsToRejected_withRejectCode() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000130");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        deliverCommand(
                new ApplyExecutionReportCommand(
                        2L, orderId, 0L, 0L, ANY_TIMESTAMP_MS,
                        /* msgSeqNum = */ 0,
                        ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                        /* rejectCodeOrZero = */ (byte) 17,
                        "VENUE", "EXEC-RJX-1", "", "{}"),
                ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).hasSize(1);
        ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)), 0, capturedAdmittedEvents.get(0).length);
        assertThat(ev.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT);
        assertThat(ev.newStatusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_REJECTED);
        assertThat(ev.rejectCodeOrZero()).isEqualTo((byte) 17);
    }

    @Test
    void applyExecutionReport_duplicateVenueExecRef_isIdempotent() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000140");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        ApplyExecutionReportCommand first = sampleTrade(orderId, 4_000_000_000L, 100_000_000L, "EXEC-DUP");
        deliverCommand(first, ANY_TIMESTAMP_MS + 1);
        assertThat(capturedAdmittedEvents).hasSize(1);

        // Same (orderId, venueExecRef) — must be a no-op (no event, no version bump, no cum-qty
        // double-application).
        deliverCommand(first, ANY_TIMESTAMP_MS + 2);
        assertThat(capturedAdmittedEvents).hasSize(1);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.cumQtyScaled()).isEqualTo(4_000_000_000L);
        assertThat(o.version()).isEqualTo(1);
        assertThat(service.hasAppliedExecutionRef(orderId, "EXEC-DUP")).isTrue();
    }

    @Test
    void applyExecutionReport_terminalOrder_isSilentlyIgnored() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000150");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(sampleCancel(orderId, "EXEC-CXL-FIRST"), ANY_TIMESTAMP_MS + 1);
        capturedAdmittedEvents.clear();

        // Order is already CANCELLED — late venue trade ER must be a no-op and not flap state back
        // to a live status.
        deliverCommand(
                sampleTrade(orderId, 1_000_000_000L, 100_000_000L, "EXEC-LATE-TRADE"),
                ANY_TIMESTAMP_MS + 2);

        assertThat(capturedAdmittedEvents).isEmpty();
        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_CANCELLED);
        assertThat(o.version()).isEqualTo(1);
    }

    @Test
    void applyExecutionReport_unknownOrder_isSilentlyIgnored() {
        UUID unknownOrderId = UUID.fromString("00000000-0000-4000-8000-000000000160");

        deliverCommand(
                sampleTrade(unknownOrderId, 1_000_000_000L, 100_000_000L, "EXEC-UNKNOWN"),
                ANY_TIMESTAMP_MS);

        assertThat(capturedAdmittedEvents).isEmpty();
        assertThat(service.lookupByOrderId(unknownOrderId)).isNull();
    }

    @Test
    void applyExecutionReport_duplicateSenderSeq_isWireLevelDedupe() {
        // Slice 3d wire-level dedupe: a broker resend of the same MsgSeqNum after a session-level
        // gap MUST NOT re-fill the order, even if it carries a fresh ExecID. We verify by sending
        // two commands with the same (senderCompId, msgSeqNum) but different venueExecRef and
        // different lastQty values; only the first apply takes effect on the order state.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000165");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);

        ApplyExecutionReportCommand first = sampleTradeWithSenderSeq(
                orderId, /* lastQtyScaled = */ 4_000_000_000L, "EXEC-FIRST", "BROKER_X", 42);
        ApplyExecutionReportCommand resend = sampleTradeWithSenderSeq(
                orderId, /* lastQtyScaled = */ 9_000_000_000L, "EXEC-RESEND", "BROKER_X", 42);

        deliverCommand(first, ANY_TIMESTAMP_MS + 1);
        int eventsAfterFirst = capturedAdmittedEvents.size();

        deliverCommand(resend, ANY_TIMESTAMP_MS + 2);

        assertThat(capturedAdmittedEvents)
                .as("wire-level dedupe drops the resend before any state mutation; no second event")
                .hasSize(eventsAfterFirst);
        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.cumQtyScaled())
                .as("only the first apply contributed to cumQty (4e9), the resend was dropped")
                .isEqualTo(4_000_000_000L);
        assertThat(o.version()).isEqualTo(1);
        assertThat(service.hasAppliedSenderSeq("BROKER_X", 42)).isTrue();
    }

    @Test
    void applyExecutionReport_distinctSenderOrSeq_areAppliedIndependently() {
        // Wire dedupe is keyed on the (senderCompId, msgSeqNum) pair: distinct seq from same
        // sender, OR same seq from a different sender, both pass through.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000166");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);

        deliverCommand(
                sampleTradeWithSenderSeq(orderId, 1_000_000_000L, "EXEC-A", "BROKER_X", 1),
                ANY_TIMESTAMP_MS + 1);
        deliverCommand(
                sampleTradeWithSenderSeq(orderId, 2_000_000_000L, "EXEC-B", "BROKER_X", 2),
                ANY_TIMESTAMP_MS + 2);
        deliverCommand(
                sampleTradeWithSenderSeq(orderId, 3_000_000_000L, "EXEC-C", "BROKER_Y", 1),
                ANY_TIMESTAMP_MS + 3);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.cumQtyScaled())
                .as("all three fills counted (no dedupe collision): 1e9 + 2e9 + 3e9")
                .isEqualTo(6_000_000_000L);
        assertThat(o.version()).isEqualTo(3);
        assertThat(service.hasAppliedSenderSeq("BROKER_X", 1)).isTrue();
        assertThat(service.hasAppliedSenderSeq("BROKER_X", 2)).isTrue();
        assertThat(service.hasAppliedSenderSeq("BROKER_Y", 1)).isTrue();
        assertThat(service.hasAppliedSenderSeq("BROKER_Y", 2)).isFalse();
    }

    @Test
    void applyExecutionReport_emptySenderCompId_optsOutOfWireDedupe() {
        // The slice 3c codec tests and any cluster-internal callers pass senderCompId="";
        // wire-level dedupe MUST NOT engage in that case (the (orderId, venueExecRef) FIX-level
        // guard is sufficient for those callers). Two trades with different ExecIDs but both
        // carrying empty senderCompId apply normally.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000167");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);

        deliverCommand(
                sampleTrade(orderId, 1_000_000_000L, 100_000_000L, "EXEC-X-1"),
                ANY_TIMESTAMP_MS + 1);
        deliverCommand(
                sampleTrade(orderId, 1_000_000_000L, 100_000_000L, "EXEC-X-2"),
                ANY_TIMESTAMP_MS + 2);

        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.cumQtyScaled()).isEqualTo(2_000_000_000L);
        assertThat(o.version()).isEqualTo(2);
        // The empty-sender index is never written to.
        assertThat(service.hasAppliedSenderSeq("", 0)).isFalse();
    }

    @Test
    void snapshot_includesSenderSeqIndex_resendAfterRestore_isStillDedupedOnWire() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000168");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(
                sampleTradeWithSenderSeq(orderId, 4_000_000_000L, "EXEC-WIRE-1", "BROKER_W", 7),
                ANY_TIMESTAMP_MS + 1);

        byte[] snapshotBytes = takeSnapshotBytes(service);
        OmsAdmissionClusteredService restored = newServiceFromSnapshot(snapshotBytes);

        assertThat(restored.hasAppliedSenderSeq("BROKER_W", 7))
                .as("wire-dedupe set survives snapshot/restore (slice 3d v3 schema)")
                .isTrue();

        // Re-deliver the exact (sender, seq) with a fresh ExecID — the (orderId, venueExecRef)
        // guard would have allowed the second apply, but the (sender, seq) guard catches it.
        ExclusivePublication restoredEventsPub =
                (ExclusivePublication) lastAddedExclusivePublication(restored);
        org.mockito.Mockito.reset(restoredEventsPub);
        when(restoredEventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        deliverCommandTo(
                restored,
                sampleTradeWithSenderSeq(orderId, 4_000_000_000L, "EXEC-FRESH-EXECID", "BROKER_W", 7),
                ANY_TIMESTAMP_MS + 100);
        org.mockito.Mockito.verify(restoredEventsPub, org.mockito.Mockito.never())
                .offer(any(DirectBuffer.class), anyInt(), anyInt());

        OmsAdmissionClusteredService.AdmittedOrder o = restored.lookupByOrderId(orderId);
        assertThat(o.cumQtyScaled())
                .as("re-delivered ER did not bump cumQty after restore — wire dedupe held")
                .isEqualTo(4_000_000_000L);
        assertThat(o.version()).isEqualTo(1);
    }

    @Test
    void snapshot_includesExecutionRefIndex_andPostFillOrderState() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000170");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(sampleTrade(orderId, 4_000_000_000L, 100_000_000L, "EXEC-S-1"), ANY_TIMESTAMP_MS + 1);
        deliverCommand(sampleTrade(orderId, 3_000_000_000L, 100_000_000L, "EXEC-S-2"), ANY_TIMESTAMP_MS + 2);

        byte[] snapshotBytes = takeSnapshotBytes(service);
        OmsAdmissionClusteredService restored = newServiceFromSnapshot(snapshotBytes);

        // Order state survived the snapshot byte-for-byte.
        OmsAdmissionClusteredService.AdmittedOrder o = restored.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_PARTIALLY_FILLED);
        assertThat(o.cumQtyScaled()).isEqualTo(7_000_000_000L);
        assertThat(o.version()).isEqualTo(2);

        // Execution-ref dedupe set survived: re-delivering EXEC-S-1 / EXEC-S-2 against the
        // restored service must be a no-op. Drive the re-delivery through onSessionMessage on the
        // restored instance and assert no new events emitted.
        ExclusivePublication restoredEventsPub =
                (ExclusivePublication) lastAddedExclusivePublication(restored);
        org.mockito.Mockito.reset(restoredEventsPub);
        when(restoredEventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        deliverCommandTo(restored, sampleTrade(orderId, 4_000_000_000L, 100_000_000L, "EXEC-S-1"),
                ANY_TIMESTAMP_MS + 100);
        deliverCommandTo(restored, sampleTrade(orderId, 3_000_000_000L, 100_000_000L, "EXEC-S-2"),
                ANY_TIMESTAMP_MS + 101);
        org.mockito.Mockito.verify(restoredEventsPub, org.mockito.Mockito.never())
                .offer(any(DirectBuffer.class), anyInt(), anyInt());

        // And confirm the helper is consistent with hasAppliedExecutionRef.
        assertThat(restored.hasAppliedExecutionRef(orderId, "EXEC-S-1")).isTrue();
        assertThat(restored.hasAppliedExecutionRef(orderId, "EXEC-S-2")).isTrue();
        assertThat(restored.hasAppliedExecutionRef(orderId, "NEVER-SEEN")).isFalse();
    }

    @Test
    void replay_reproducesIdenticalEventsStream_byteForByte() {
        // Determinism guard: a replay over the same command sequence must produce the same bytes
        // on the events publication. This protects against any non-deterministic field (timestamp,
        // version, ordering) sneaking into ExecutionAppliedEvent or OrderAdmittedEvent.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000180");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(sampleTrade(orderId, 4_000_000_000L, 100_000_000L, "EXEC-R-1"), ANY_TIMESTAMP_MS + 1);
        deliverCommand(sampleCancel(orderId, "EXEC-R-CXL"), ANY_TIMESTAMP_MS + 2);
        java.util.List<byte[]> firstRunEvents = new java.util.ArrayList<>(capturedAdmittedEvents);

        ReplayHarness replay = new ReplayHarness();
        replay.deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        replay.deliverCommand(sampleTrade(orderId, 4_000_000_000L, 100_000_000L, "EXEC-R-1"), ANY_TIMESTAMP_MS + 1);
        replay.deliverCommand(sampleCancel(orderId, "EXEC-R-CXL"), ANY_TIMESTAMP_MS + 2);

        assertThat(replay.captured).hasSameSizeAs(firstRunEvents);
        for (int i = 0; i < firstRunEvents.size(); i++) {
            assertThat(replay.captured.get(i))
                    .as("event %d byte-equal across runs", i)
                    .isEqualTo(firstRunEvents.get(i));
        }
    }

    private static ApplyExecutionReportCommand sampleTrade(
            UUID orderId, long lastQtyScaled, long lastPxScaled, String venueExecRef) {
        return new ApplyExecutionReportCommand(
                /* correlationId = */ 0L,
                orderId,
                lastQtyScaled,
                lastPxScaled,
                /* venueTsNanos = */ 1L,
                /* msgSeqNum = */ 0,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                "VENUE",
                venueExecRef,
                /* senderCompId = */ "",
                "{}");
    }

    /**
     * Slice 3d helper: trade command with the wire-level dedupe key
     * {@code (senderCompId, msgSeqNum)} populated. Tests that exercise wire dedupe build their
     * commands through this; tests that opt out (slice 3c-style) keep using {@link #sampleTrade}.
     */
    private static ApplyExecutionReportCommand sampleTradeWithSenderSeq(
            UUID orderId,
            long lastQtyScaled,
            String venueExecRef,
            String senderCompId,
            int msgSeqNum) {
        return new ApplyExecutionReportCommand(
                0L,
                orderId,
                lastQtyScaled,
                /* lastPxScaled = */ 100_000_000L,
                1L,
                msgSeqNum,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                (byte) 0,
                "VENUE",
                venueExecRef,
                senderCompId,
                "{}");
    }

    private static ApplyExecutionReportCommand sampleCancel(UUID orderId, String venueExecRef) {
        return new ApplyExecutionReportCommand(
                0L, orderId, 0L, 0L, 1L, 0,
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL,
                (byte) 0, "VENUE", venueExecRef, "", "{}");
    }

    /** Build a fresh service primed from {@code snapshotBytes}. Uses a stub events publication. */
    private static OmsAdmissionClusteredService newServiceFromSnapshot(byte[] snapshotBytes) {
        OmsAdmissionClusteredService restored = new OmsAdmissionClusteredService();
        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        Aeron aeronMock = mock(Aeron.class);
        ExclusivePublication eventsPub = mock(ExclusivePublication.class);
        when(eventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        OmsAdmissionClusteredServiceTestFixtures.wireClusterAeronMocks(aeronMock, eventsPub);
        when(mockCluster.aeron()).thenReturn(aeronMock);
        Image snapshotImage = mockSnapshotImage(snapshotBytes);
        restored.onStart(mockCluster, snapshotImage);
        return restored;
    }

    /** Reflectively pull the events publication mock the restored service captured at onStart. */
    private static ExclusivePublication lastAddedExclusivePublication(OmsAdmissionClusteredService svc) {
        try {
            java.lang.reflect.Field f = OmsAdmissionClusteredService.class.getDeclaredField("eventsPublication");
            f.setAccessible(true);
            return (ExclusivePublication) f.get(svc);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Encode + deliver any command via {@code onSessionMessage} on an arbitrary service instance. */
    private void deliverCommandTo(
            OmsAdmissionClusteredService svc, AcceptOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        svc.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private void deliverCommandTo(
            OmsAdmissionClusteredService svc, ApplyExecutionReportCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        svc.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private void deliverCommand(ApplyExecutionReportCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private void deliverCommand(CancelOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private void deliverCommand(RequestCancelOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private void deliverCommand(RequestReplaceOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    // ------------------------------------------------------------------------
    // Slice 4p: CancelOrderCommand (OMS-initiated cancel)
    // ------------------------------------------------------------------------

    @Test
    void applyCancelOrder_workingOrder_emitsOrderCancelAppliedAndMutatesState() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000004001");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        deliverCommand(
                new CancelOrderCommand(7L, orderId, /* requestedAtNanos = */ 12345L,
                        "ledger_inflight_hold_failed:insufficient_funds"),
                ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).hasSize(1);
        OrderCancelAppliedEvent ev = OrderCancelAppliedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)),
                0,
                capturedAdmittedEvents.get(0).length);
        assertThat(ev.orderId()).isEqualTo(orderId);
        assertThat(ev.cancelledAtMillis()).isEqualTo(ANY_TIMESTAMP_MS + 1);
        assertThat(ev.newVersion()).isEqualTo(1);
        assertThat(ev.accountId()).isEqualTo("acct");
        assertThat(ev.reason()).isEqualTo("ledger_inflight_hold_failed:insufficient_funds");

        OmsAdmissionClusteredService.AdmittedOrder mutated = service.lookupByOrderId(orderId);
        assertThat(mutated.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_CANCELLED);
        assertThat(mutated.version()).isEqualTo(1);
    }

    @Test
    void applyCancelOrder_unknownOrder_silentNoOp() {
        UUID neverAdmitted = UUID.fromString("00000000-0000-4000-8000-000000004099");

        deliverCommand(
                new CancelOrderCommand(1L, neverAdmitted, 1L, "compensator"),
                ANY_TIMESTAMP_MS);

        assertThat(capturedAdmittedEvents).isEmpty();
        assertThat(capturedEgress).isEmpty();
        assertThat(service.admittedOrderCount()).isZero();
        assertThat(meterRegistry.get("oms.cluster.cancel_unknown_order_total").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void applyCancelOrder_alreadyFilled_silentNoOpAndKeepsTerminalState() {
        // Documented race: a venue fill lands between the inflight-hold failure and the
        // compensator's cancel. Cluster sees the order is terminal and silent no-ops; the
        // compensator's job is to "try to cancel, do not retry", which it has done.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000004102");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(
                sampleTrade(orderId, /* lastQtyScaled = */ 10_000_000_000L, 100_000_000L, "EXEC-FILL"),
                ANY_TIMESTAMP_MS + 1);
        capturedAdmittedEvents.clear();

        deliverCommand(
                new CancelOrderCommand(2L, orderId, 1L, "compensator"),
                ANY_TIMESTAMP_MS + 2);

        assertThat(capturedAdmittedEvents).isEmpty();
        OmsAdmissionClusteredService.AdmittedOrder o = service.lookupByOrderId(orderId);
        assertThat(o.statusCode()).isEqualTo(OmsAdmissionClusteredService.STATUS_FILLED);
    }

    @Test
    void snapshot_roundtripPreservesNonZeroShardId() {
        // Phase 4 Tier 2.5 phase E-3b: AdmittedOrder gained a shardId field and the snapshot
        // schema bumped 3 -> 4. Pin the on-wire byte layout so a future codec change that
        // forgets shardId (or drops it on decode) is caught here rather than at first replay
        // on a multi-shard cluster. The test admits at shardId=2, takes a snapshot, restores
        // a fresh service from those bytes, and asserts the restored AdmittedOrder still
        // reports shardId=2.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000004301");
        deliverCommand(
                sampleAccept(11L, "acct-shard2", "idem-shard2", orderId, /* shardId = */ 2),
                ANY_TIMESTAMP_MS);

        byte[] snapshotBytes = takeSnapshotBytes(service);
        OmsAdmissionClusteredService restored = newServiceFromSnapshot(snapshotBytes);

        OmsAdmissionClusteredService.AdmittedOrder o = restored.lookupByOrderId(orderId);
        assertThat(o).as("admitted order survives snapshot roundtrip").isNotNull();
        assertThat(o.shardId())
                .as("AdmittedOrder.shardId must round-trip through SNAPSHOT_SCHEMA_VERSION=4 unchanged")
                .isEqualTo(2);
    }

    @Test
    void applyCancelOrder_emitsEventWithOrderShardId_notHardcodedZero() {
        // Phase 4 Tier 2.5 phase E-3b: the cluster service must propagate the order's owning
        // shardId (seeded at admit time from cmd.shardId()) onto OrderCancelAppliedEvent so the
        // E-2 projector shard guard accepts it on shard != 0. Pre-E-3b the emit hardcoded
        // shardId=0 and a non-zero-shard projector would silently drop the cancel event.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000004201");
        deliverCommand(
                sampleAccept(99L, "acct", "idem", orderId, /* shardId = */ 1),
                ANY_TIMESTAMP_MS);
        capturedAdmittedEvents.clear();

        deliverCommand(
                new CancelOrderCommand(101L, orderId, 1L, "ledger_inflight_hold_failed:test"),
                ANY_TIMESTAMP_MS + 1);

        assertThat(capturedAdmittedEvents).hasSize(1);
        OrderCancelAppliedEvent ev = OrderCancelAppliedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(0)),
                0,
                capturedAdmittedEvents.get(0).length);
        assertThat(ev.shardId())
                .as("OrderCancelAppliedEvent must carry the order's actual shardId from admission, not 0")
                .isEqualTo(1);
        assertThat(ev.orderId()).isEqualTo(orderId);
    }

    @Test
    void applyCancelOrder_replay_isIdempotent() {
        // Cluster log replay re-delivers the same CancelOrderCommand. The first apply mutates
        // working -> CANCELLED; the second sees the order is terminal and silent no-ops. State
        // is byte-equal across the second attempt.
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000004110");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(
                new CancelOrderCommand(2L, orderId, 1L, "first"),
                ANY_TIMESTAMP_MS + 1);
        OmsAdmissionClusteredService.AdmittedOrder afterFirst = service.lookupByOrderId(orderId);
        int eventsAfterFirst = capturedAdmittedEvents.size();

        deliverCommand(
                new CancelOrderCommand(2L, orderId, 1L, "first"),
                ANY_TIMESTAMP_MS + 2);

        OmsAdmissionClusteredService.AdmittedOrder afterSecond = service.lookupByOrderId(orderId);
        assertThat(afterSecond).isEqualTo(afterFirst);
        assertThat(capturedAdmittedEvents).hasSize(eventsAfterFirst);
    }

    /**
     * A second standalone service instance for replay-equivalence assertions. Built fresh so it has
     * no leftover state from {@link #service}. Captures every events-publication offer into
     * {@link #captured} for byte-equal comparison.
     */
    private final class ReplayHarness {
        final java.util.List<byte[]> captured = new java.util.ArrayList<>();
        private final OmsAdmissionClusteredService svc = new OmsAdmissionClusteredService();
        private final ClientSession sessionMock = mock(ClientSession.class);

        ReplayHarness() {
            when(sessionMock.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
            ExclusivePublication eventsPub = mock(ExclusivePublication.class);
            when(eventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(inv -> {
                DirectBuffer buf = inv.getArgument(0);
                int off = inv.getArgument(1);
                int len = inv.getArgument(2);
                byte[] copy = new byte[len];
                buf.getBytes(off, copy);
                captured.add(copy);
                return 1L;
            });
            Aeron aeronMock = mock(Aeron.class);
            OmsAdmissionClusteredServiceTestFixtures.wireClusterAeronMocks(aeronMock, eventsPub);
            Cluster clusterMock = mock(Cluster.class);
            when(clusterMock.role()).thenReturn(Cluster.Role.LEADER);
            when(clusterMock.aeron()).thenReturn(aeronMock);
            svc.onStart(clusterMock, /* snapshotImage = */ null);
        }

        void deliverCommand(AcceptOrderCommand cmd, long clusterTimestampMillis) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            int written = cmd.encode(buffer, 0);
            svc.onSessionMessage(sessionMock, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
        }

        void deliverCommand(ApplyExecutionReportCommand cmd, long clusterTimestampMillis) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            int written = cmd.encode(buffer, 0);
            svc.onSessionMessage(sessionMock, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
        }
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private void deliverCommand(AcceptOrderCommand cmd, long clusterTimestampMillis) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        service.onSessionMessage(session, clusterTimestampMillis, buffer, 0, written, /* header = */ null);
    }

    private static AcceptOrderCommand sampleAccept(long correlationId, String accountId, String idemKey, UUID orderId) {
        return sampleAccept(correlationId, accountId, idemKey, orderId, /* shardId = */ 0);
    }

    private static AcceptOrderCommand sampleAccept(
            long correlationId, String accountId, String idemKey, UUID orderId, int shardId) {
        return new AcceptOrderCommand(
                correlationId,
                orderId,
                /* clientTimestampNanos = */ 0L,
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                shardId,
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
     * Build a mocked {@link Image} that delivers {@code snapshotBytes} as a single complete
     * fragment then reports end-of-stream. The supplied {@link io.aeron.logbuffer.Header} reports
     * {@link io.aeron.logbuffer.FrameDescriptor#UNFRAGMENTED} so the {@link io.aeron.ImageFragmentAssembler}
     * that wraps the loader in {@code loadSnapshot} short-circuits to the delegate (single-fragment
     * pass-through). For multi-fragment behaviour see
     * {@code OmsClusterChaosIT#largeSnapshotSurvivesFragmentationAcrossRestart}.
     */
    private static Image mockSnapshotImage(byte[] snapshotBytes) {
        Image image = mock(Image.class);
        AtomicBoolean delivered = new AtomicBoolean(false);
        when(image.isEndOfStream()).thenAnswer(inv -> delivered.get());
        io.aeron.logbuffer.Header header = mock(io.aeron.logbuffer.Header.class);
        when(header.flags()).thenReturn(io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED);
        when(image.poll(any(FragmentHandler.class), anyInt())).thenAnswer(inv -> {
            if (delivered.get()) {
                return 0;
            }
            FragmentHandler handler = inv.getArgument(0);
            UnsafeBuffer buffer = new UnsafeBuffer(snapshotBytes);
            handler.onFragment(buffer, 0, snapshotBytes.length, header);
            delivered.set(true);
            return 1;
        });
        return image;
    }

    // ========================================================================
    // Wed-demo: RequestCancelOrder / RequestReplaceOrder / ER ET=5 + 35=9 paths
    // ========================================================================

    @Test
    void applyRequestCancelOrder_liveOrder_emitsCancelRequestedEvent_andDoesNotMutateStatus() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005001");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        OmsAdmissionClusteredService.AdmittedOrder beforeRequest = service.lookupByOrderId(orderId);
        int eventsBefore = capturedAdmittedEvents.size();

        deliverCommand(
                new RequestCancelOrderCommand(2L, orderId, 1L, "idem-cancel-1", "user-cancel"),
                ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder afterRequest = service.lookupByOrderId(orderId);
        assertThat(afterRequest.statusCode())
                .as("order status must NOT change on RequestCancel — wait for broker ER")
                .isEqualTo(beforeRequest.statusCode());
        assertThat(afterRequest.version())
                .as("RequestCancel must not bump version (no orders-row change to project)")
                .isEqualTo(beforeRequest.version());
        assertThat(capturedAdmittedEvents).hasSize(eventsBefore + 1);

        OrderCancelRequestedEvent ev = OrderCancelRequestedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(eventsBefore)),
                0,
                capturedAdmittedEvents.get(eventsBefore).length);
        assertThat(ev.orderId()).isEqualTo(orderId);
        assertThat(ev.clientRequestKey()).isEqualTo("idem-cancel-1");
        assertThat(ev.reason()).isEqualTo("user-cancel");
        assertThat(ev.sideCode())
                .as("egress needs Side(54) for FIX 35=F — carry it on the event so no Postgres lookup")
                .isEqualTo(beforeRequest.side());
        assertThat(ev.originalQuantityScaled())
                .as("egress needs OrderQty(38) for FIX 35=F — must equal the order's original NOS qty")
                .isEqualTo(beforeRequest.quantityScaled());
        assertThat(ev.cumQtyScaled())
                .as("carried so the egress can compute LeavesQty(151) for brokers that expect it")
                .isEqualTo(beforeRequest.cumQtyScaled());
        assertThat(ev.accountId()).isEqualTo(beforeRequest.accountId());
        assertThat(ev.instrumentSymbol()).isEqualTo(beforeRequest.instrumentSymbol());
    }

    @Test
    void applyRequestCancelOrder_unknownOrder_silentNoOp() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005002");

        deliverCommand(
                new RequestCancelOrderCommand(1L, orderId, 1L, "idem-cancel", "race"),
                ANY_TIMESTAMP_MS);

        assertThat(capturedAdmittedEvents).isEmpty();
        assertThat(service.lookupByOrderId(orderId)).isNull();
    }

    @Test
    void applyRequestCancelOrder_duplicateClientRequestKey_silentNoOp() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005003");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        deliverCommand(
                new RequestCancelOrderCommand(2L, orderId, 1L, "dup-key", "first"),
                ANY_TIMESTAMP_MS + 1);
        int eventsAfterFirst = capturedAdmittedEvents.size();

        deliverCommand(
                new RequestCancelOrderCommand(3L, orderId, 1L, "dup-key", "second"),
                ANY_TIMESTAMP_MS + 2);

        assertThat(capturedAdmittedEvents)
                .as("re-delivered RequestCancel with same clientRequestKey must not double-emit")
                .hasSize(eventsAfterFirst);
    }

    @Test
    void applyRequestReplaceOrder_liveOrder_emitsReplaceRequestedEvent_andDoesNotMutateQtyOrPrice() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005010");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        OmsAdmissionClusteredService.AdmittedOrder beforeRequest = service.lookupByOrderId(orderId);
        int eventsBefore = capturedAdmittedEvents.size();

        deliverCommand(
                new RequestReplaceOrderCommand(
                        2L, orderId, /* newQty = */ 5_000_000_000L,
                        /* newLimitPx = */ 200_000_000L, 1L, "idem-rep-1", "operator-resize"),
                ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder afterRequest = service.lookupByOrderId(orderId);
        assertThat(afterRequest.quantityScaled())
                .as("qty must NOT change on RequestReplace — wait for broker ER ET=5")
                .isEqualTo(beforeRequest.quantityScaled());
        assertThat(afterRequest.limitPriceScaledOrZero())
                .as("price must NOT change on RequestReplace")
                .isEqualTo(beforeRequest.limitPriceScaledOrZero());
        assertThat(afterRequest.version())
                .isEqualTo(beforeRequest.version());

        assertThat(capturedAdmittedEvents).hasSize(eventsBefore + 1);
        OrderReplaceRequestedEvent ev = OrderReplaceRequestedEvent.decode(
                new UnsafeBuffer(capturedAdmittedEvents.get(eventsBefore)),
                0,
                capturedAdmittedEvents.get(eventsBefore).length);
        assertThat(ev.orderId()).isEqualTo(orderId);
        assertThat(ev.newQuantityScaled()).isEqualTo(5_000_000_000L);
        assertThat(ev.newLimitPriceScaledOrZero()).isEqualTo(200_000_000L);
        assertThat(ev.originalQuantityScaled()).isEqualTo(beforeRequest.quantityScaled());
    }

    @Test
    void applyRequestReplaceOrder_newQtyBelowCumQty_silentNoOp() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005011");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        // sampleAccept seeds qty = 10_000_000_000. Apply a partial fill of 6_000_000_000 first.
        deliverCommand(
                new ApplyExecutionReportCommand(
                        0L, orderId, 6_000_000_000L, 100_000_000L, 1L, 1,
                        ApplyExecutionReportCommand.EXEC_TYPE_TRADE, (byte) 0,
                        "v1", "exec-partial-1", "S", "{}"),
                ANY_TIMESTAMP_MS + 1);
        int eventsAfterFill = capturedAdmittedEvents.size();

        // Request a replace down to 5_000_000_000 — below the already-filled cumQty.
        deliverCommand(
                new RequestReplaceOrderCommand(
                        2L, orderId, /* newQty = */ 5_000_000_000L, 0L, 1L,
                        "idem-rep-impossible", ""),
                ANY_TIMESTAMP_MS + 2);

        assertThat(capturedAdmittedEvents)
                .as("cumQty-overflow RequestReplace must silently drop, no event emission")
                .hasSize(eventsAfterFill);
    }

    @Test
    void applyExecutionReport_execTypeReplace_updatesQtyAndPriceAndStatus_emitsEvent() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005020");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        OmsAdmissionClusteredService.AdmittedOrder beforeReplace = service.lookupByOrderId(orderId);
        int eventsBefore = capturedAdmittedEvents.size();

        deliverCommand(
                new ApplyExecutionReportCommand(
                        0L, orderId,
                        /* lastQtyScaled (overloaded: newOrderQty) = */ 7_500_000_000L,
                        /* lastPxScaled (overloaded: newLimitPx) = */ 150_000_000L,
                        1L, 2,
                        ApplyExecutionReportCommand.EXEC_TYPE_REPLACE, (byte) 0,
                        "v1", "exec-replace-1", "BROKER_ACCEPT",
                        "{\"kind\":\"ExecutionReport\",\"execType\":\"REPLACE\"}"),
                ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder afterReplace = service.lookupByOrderId(orderId);
        assertThat(afterReplace.quantityScaled())
                .as("REPLACE ER must update quantityScaled to the new total")
                .isEqualTo(7_500_000_000L);
        assertThat(afterReplace.limitPriceScaledOrZero())
                .as("REPLACE ER with non-zero price must update limitPriceScaled")
                .isEqualTo(150_000_000L);
        assertThat(afterReplace.version())
                .as("REPLACE ER must bump version (orders-row mutation)")
                .isEqualTo(beforeReplace.version() + 1);
        assertThat(afterReplace.statusCode())
                .as("REPLACE with no fill keeps order WORKING")
                .isEqualTo(beforeReplace.statusCode());
        assertThat(capturedAdmittedEvents).hasSize(eventsBefore + 1);
    }

    @Test
    void applyExecutionReport_execTypeCancelReject_doesNotMutateStatus_butBumpsVersionAndEmits() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005030");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        OmsAdmissionClusteredService.AdmittedOrder beforeReject = service.lookupByOrderId(orderId);
        int eventsBefore = capturedAdmittedEvents.size();

        deliverCommand(
                new ApplyExecutionReportCommand(
                        0L, orderId, 0L, 0L, 1L, 3,
                        ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT, (byte) 0,
                        "v1", "ocr-broker-1", "BROKER_ACCEPT",
                        "{\"kind\":\"OrderCancelReject\",\"cxlRejReason\":1}"),
                ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder afterReject = service.lookupByOrderId(orderId);
        assertThat(afterReject.statusCode())
                .as("35=9 CANCEL_REJECT must NOT move the order to REJECTED — it stays in its prior state")
                .isEqualTo(beforeReject.statusCode());
        assertThat(afterReject.cumQtyScaled()).isEqualTo(beforeReject.cumQtyScaled());
        assertThat(afterReject.quantityScaled()).isEqualTo(beforeReject.quantityScaled());
        assertThat(afterReject.version())
                .as("version must still bump so the projector sees the event and writes the toast outbox row")
                .isEqualTo(beforeReject.version() + 1);
        assertThat(capturedAdmittedEvents).hasSize(eventsBefore + 1);
    }

    @Test
    void applyExecutionReport_execTypeReplaceReject_doesNotMutateStatus_butBumpsVersion() {
        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000005031");
        deliverCommand(sampleAccept(1L, "acct", "idem", orderId), ANY_TIMESTAMP_MS);
        OmsAdmissionClusteredService.AdmittedOrder beforeReject = service.lookupByOrderId(orderId);

        deliverCommand(
                new ApplyExecutionReportCommand(
                        0L, orderId, 0L, 0L, 1L, 4,
                        ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT, (byte) 0,
                        "v1", "ocr-broker-2", "BROKER_ACCEPT",
                        "{\"kind\":\"OrderCancelReject\",\"cxlRejReason\":2}"),
                ANY_TIMESTAMP_MS + 1);

        OmsAdmissionClusteredService.AdmittedOrder afterReject = service.lookupByOrderId(orderId);
        assertThat(afterReject.statusCode()).isEqualTo(beforeReject.statusCode());
        assertThat(afterReject.quantityScaled()).isEqualTo(beforeReject.quantityScaled());
        assertThat(afterReject.version()).isEqualTo(beforeReject.version() + 1);
    }

    @Test
    void firstRoleChange_logsReplayValidationOnce() {
        assertThat(service.replayValidationLoggedForTest()).isFalse();
        service.onRoleChange(Cluster.Role.FOLLOWER);
        assertThat(service.replayValidationLoggedForTest()).isTrue();
        service.onRoleChange(Cluster.Role.LEADER);
        assertThat(service.replayValidationLoggedForTest()).isTrue();
    }

}
