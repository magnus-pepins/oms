package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Objects;
import java.util.UUID;

/**
 * Cluster egress event: emitted by {@link OmsAdmissionClusteredService} when an
 * {@link AcceptOrderCommand} is admitted (either freshly accepted, or matched
 * to a prior idempotent admission).
 *
 * <p>Subscribers (cluster client returning HTTP 201, Postgres projector, FIX
 * egress queue) read these events and apply their side-effects.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long  orderIdMsb
 *   offset 24  long  orderIdLsb
 *   offset 32  int   version           (orders.version after CAS)
 *   offset 36  byte  duplicate          (0 == fresh, 1 == idempotent re-hit)
 *   offset 37  byte  reserved (3 bytes padding to align)
 *   offset 40  long  acceptedAtNanos    (cluster-supplied wall time)
 * </pre>
 */
public record OrderAcceptedEvent(
        long correlationId,
        UUID orderId,
        int version,
        boolean duplicate,
        long acceptedAtNanos) {

    public OrderAcceptedEvent {
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Encoded length on the wire (fixed). */
    public static final int ENCODED_LENGTH = OmsClusterWireFormat.HEADER_LENGTH + 32;

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putInt(p, version);
        p += Integer.BYTES;
        buffer.putByte(p++, (byte) (duplicate ? 1 : 0));
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);
        buffer.putByte(p++, (byte) 0);
        buffer.putLong(p, acceptedAtNanos);
        p += Long.BYTES;

        return p - offset;
    }

    public static OrderAcceptedEvent decode(DirectBuffer buffer, int offset) {
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        long correlationId = buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);
        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        int version = buffer.getInt(p);
        p += Integer.BYTES;
        boolean duplicate = buffer.getByte(p) == 1;
        p += 4;
        long acceptedAtNanos = buffer.getLong(p);
        return new OrderAcceptedEvent(correlationId, new UUID(msb, lsb), version, duplicate, acceptedAtNanos);
    }
}
