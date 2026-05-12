package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link ExecutionAppliedEvent}.
 *
 * <p>The events recording is the projector's source of truth (slice 3e folds the projector
 * consumer); a corrupt encoding here desynchronises Postgres from the cluster permanently. Same
 * shape as {@link OrderAdmittedEventCodecTest}.
 */
class ExecutionAppliedEventCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_partialFill_decodesIdenticalRecord() {
        ExecutionAppliedEvent original = new ExecutionAppliedEvent(
                UUID.fromString("a1b2c3d4-e5f6-4789-9abc-def012345678"),
                /* newCumQtyScaled = */ 5_000_000_000L,
                /* lastQtyScaled = */ 5_000_000_000L,
                /* lastPxScaled = */ 195_500_000L,
                /* venueTsNanos = */ 1_700_000_000_111_222_333L,
                /* appliedAtNanos = */ 1_700_000_000_999_888_777L,
                /* newVersion = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                OmsAdmissionClusteredService.STATUS_PARTIALLY_FILLED,
                /* rejectCodeOrZero = */ (byte) 0,
                "acct-12345",
                "VENUE-X",
                "EXEC-12345",
                "{\"kind\":\"ExecutionReport\"}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        ExecutionAppliedEvent decoded = ExecutionAppliedEvent.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_terminalCancel_decodesIdenticalRecord() {
        ExecutionAppliedEvent original = new ExecutionAppliedEvent(
                UUID.fromString("00000000-0000-4000-8000-000000000010"),
                /* newCumQtyScaled = */ 0L,
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                /* venueTsNanos = */ 1L,
                /* appliedAtNanos = */ 2L,
                /* newVersion = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL,
                OmsAdmissionClusteredService.STATUS_CANCELLED,
                (byte) 0,
                "acct",
                "V",
                "X",
                "{}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        assertThat(ExecutionAppliedEvent.decode(buf, DECODE_OFFSET, written)).isEqualTo(original);
    }

    @Test
    void roundTrip_terminalVenueReject_decodesIdenticalRecord() {
        ExecutionAppliedEvent original = new ExecutionAppliedEvent(
                UUID.fromString("00000000-0000-4000-8000-000000000020"),
                /* newCumQtyScaled = */ 0L,
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                /* venueTsNanos = */ 1L,
                /* appliedAtNanos = */ 2L,
                /* newVersion = */ 1,
                ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                OmsAdmissionClusteredService.STATUS_REJECTED,
                /* rejectCodeOrZero = */ (byte) 17,
                "acct",
                "V",
                "X",
                "{}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        assertThat(ExecutionAppliedEvent.decode(buf, DECODE_OFFSET, written)).isEqualTo(original);
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> ExecutionAppliedEvent.decode(
                        buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void decode_bufferShorterThanHeader_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> ExecutionAppliedEvent.decode(
                        buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for header");
    }
}
