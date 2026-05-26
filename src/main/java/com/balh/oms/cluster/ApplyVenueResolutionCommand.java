package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Phase B cluster command: apply a venue binary contract resolution to OMS admission state.
 *
 * <p>Idempotent on {@code (instrumentSymbol, evidenceHash)}. Emits {@link VenueResolutionAppliedEvent}
 * on first apply; replays are silent no-ops.
 */
public record ApplyVenueResolutionCommand(
        long correlationId,
        String instrumentSymbol,
        byte outcomeCode,
        String resolutionSource,
        long resolutionTimestampMillis,
        String evidenceHash,
        String venueId) {

    public ApplyVenueResolutionCommand {
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
        Objects.requireNonNull(resolutionSource, "resolutionSource");
        Objects.requireNonNull(evidenceHash, "evidenceHash");
        Objects.requireNonNull(venueId, "venueId");
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(
                p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET,
                OmsClusterWireFormat.TYPE_ID_APPLY_VENUE_RESOLUTION);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;
        p = writeString(buffer, p, instrumentSymbol);
        buffer.putByte(p++, outcomeCode);
        p = writeString(buffer, p, resolutionSource);
        buffer.putLong(p, resolutionTimestampMillis);
        p += Long.BYTES;
        p = writeString(buffer, p, evidenceHash);
        p = writeString(buffer, p, venueId);
        return p - offset;
    }

    public static ApplyVenueResolutionCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short");
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_APPLY_VENUE_RESOLUTION) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        long correlationId = buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);
        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        String symbol = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        byte outcome = buffer.getByte(p++);
        String source = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        long ts = buffer.getLong(p);
        p += Long.BYTES;
        String hash = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String venueId = readString(buffer, p);
        return new ApplyVenueResolutionCommand(correlationId, symbol, outcome, source, ts, hash, venueId);
    }

    public String idempotencyKey() {
        return instrumentSymbol.trim().toUpperCase() + "|" + evidenceHash.trim();
    }

    private static int writeString(MutableDirectBuffer buffer, int offset, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string too long: " + bytes.length);
        }
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + Integer.BYTES, bytes);
        return offset + Integer.BYTES + bytes.length;
    }

    private static String readString(DirectBuffer buffer, int offset) {
        int len = buffer.getInt(offset);
        byte[] bytes = new byte[len];
        buffer.getBytes(offset + Integer.BYTES, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int stringByteLenAt(DirectBuffer buffer, int offset) {
        return Integer.BYTES + buffer.getInt(offset);
    }
}
