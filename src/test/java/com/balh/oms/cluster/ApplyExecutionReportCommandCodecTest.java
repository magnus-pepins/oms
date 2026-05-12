package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip and validation tests for {@link ApplyExecutionReportCommand}.
 *
 * <p>The wire format is consumed by every cluster member (leader, followers, and any future replay
 * tooling) so corruption here breaks log durability across the whole cluster. Same shape as
 * {@link AcceptOrderCommandCodecTest}.
 */
class ApplyExecutionReportCommandCodecTest {

    private static final int ENCODE_BUFFER_CAPACITY = 1024;
    private static final int DECODE_OFFSET = 0;

    @Test
    void roundTrip_tradeFill_decodesIdenticalRecord() {
        ApplyExecutionReportCommand original = new ApplyExecutionReportCommand(
                /* correlationId = */ 0xCAFE_BABE_4242L,
                UUID.fromString("a1b2c3d4-e5f6-4789-9abc-def012345678"),
                /* lastQtyScaled = */ 5_000_000_000L,
                /* lastPxScaled = */ 195_500_000L,
                /* venueTsNanos = */ 1_700_000_000_111_222_333L,
                /* msgSeqNum = */ 0,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                "VENUE-X",
                "EXEC-12345",
                /* senderCompId = */ "",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"TRADE\"}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        ApplyExecutionReportCommand decoded =
                ApplyExecutionReportCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_cancelWithSenderAndSeqNum_decodesIdenticalRecord() {
        ApplyExecutionReportCommand original = new ApplyExecutionReportCommand(
                42L,
                UUID.fromString("00000000-0000-4000-8000-000000000001"),
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                /* venueTsNanos = */ 1L,
                /* msgSeqNum = */ 12345,
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL,
                /* rejectCodeOrZero = */ (byte) 0,
                "VENUE",
                "EXEC-CXL-1",
                "BROKER1",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"CANCEL\"}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        ApplyExecutionReportCommand decoded =
                ApplyExecutionReportCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTrip_venueRejectWithRejectCode_decodesIdenticalRecord() {
        ApplyExecutionReportCommand original = new ApplyExecutionReportCommand(
                7L,
                UUID.fromString("00000000-0000-4000-8000-000000000002"),
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                /* venueTsNanos = */ 99L,
                /* msgSeqNum = */ 7,
                ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                /* rejectCodeOrZero = */ (byte) 17,
                "VENUE",
                "EXEC-RJX-1",
                "BROKER2",
                "{\"kind\":\"ExecutionReport\",\"execType\":\"REJECTED\"}");

        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        int written = original.encode(buf, DECODE_OFFSET);

        ApplyExecutionReportCommand decoded =
                ApplyExecutionReportCommand.decode(buf, DECODE_OFFSET, written);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void encode_stringExceedingMaxBytes_throwsBeforeAnyWireWrite() {
        String tooLong = "x".repeat(OmsClusterWireFormat.MAX_STRING_BYTES + 1);
        ApplyExecutionReportCommand cmd = new ApplyExecutionReportCommand(
                1L, UUID.randomUUID(), 1L, 0L, 0L, 0,
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE, (byte) 0,
                tooLong, "ref", "", "{}");
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> cmd.encode(buf, DECODE_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds MAX_STRING_BYTES");
    }

    @Test
    void decode_unexpectedTypeId_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> ApplyExecutionReportCommand.decode(
                        buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected typeId");
    }

    @Test
    void decode_unsupportedSchemaVersion_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);
        buf.putInt(0, OmsClusterWireFormat.TYPE_ID_APPLY_EXECUTION_REPORT);
        buf.putInt(4, OmsClusterWireFormat.SCHEMA_VERSION + 1);
        buf.putLong(8, 0L);

        assertThatThrownBy(() -> ApplyExecutionReportCommand.decode(
                        buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported schema version");
    }

    @Test
    void decode_bufferShorterThanHeader_throws() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(ENCODE_BUFFER_CAPACITY);

        assertThatThrownBy(() -> ApplyExecutionReportCommand.decode(
                        buf, DECODE_OFFSET, OmsClusterWireFormat.HEADER_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short for header");
    }
}
