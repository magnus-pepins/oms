package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Cluster command: client requests admission of a new order.
 *
 * <p>This is the cluster-side analog of {@code OrderIngressService.persistAccepted}
 * in the legacy code. Carries everything the deterministic state machine needs
 * to decide accept vs reject; no external lookup is permitted from inside the
 * apply path.
 *
 * <p>Time and identifiers are <strong>supplied by the cluster client</strong>
 * (and validated at the edge), not generated inside the state machine. This is
 * what makes the apply deterministic across leader / follower / replay.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long  orderIdMsb        (UUID most-significant bits)
 *   offset 24  long  orderIdLsb        (UUID least-significant bits)
 *   offset 32  long  clientTimestampNanos  (epoch nanos, client wall-time)
 *   offset 40  long  quantityScaled        (scale 1e9; e.g. "10.5" = 10_500_000_000)
 *   offset 48  long  limitPriceScaledOrZero (scale 1e6; 0 == market order)
 *   offset 56  int   shardId
 *   offset 60  byte  side                  (0 == BUY, 1 == SELL)
 *   offset 61  byte  timeInForceCode       (0 == DAY, 1 == IOC, 2 == FOK, 3 == GTC)
 *   offset 62  byte  hasLedgerBalanceId    (0 == no, 1 == yes)
 *   offset 63  byte  reserved (must be 0)
 *   offset 64  string accountId
 *   offset N   string clientIdempotencyKey
 *   offset N   string accountIdHash         (PII-safe; computed at edge)
 *   offset N   string instrumentSymbol
 *   offset N   string ledgerBalanceId       (only when hasLedgerBalanceId == 1)
 * </pre>
 *
 * <p>Strings are length-prefixed: 4-byte int length followed by UTF-8 bytes.
 * Maximum per-string length is {@link OmsClusterWireFormat#MAX_STRING_BYTES}.
 */
public record AcceptOrderCommand(
        long correlationId,
        UUID orderId,
        long clientTimestampNanos,
        long quantityScaled,
        long limitPriceScaledOrZero,
        int shardId,
        byte side,
        byte timeInForceCode,
        String accountId,
        String clientIdempotencyKey,
        String accountIdHash,
        String instrumentSymbol,
        String ledgerBalanceIdOrNull) {

    /** Quantity scale factor: store quantities as fixed-point with 9 decimal places. */
    public static final long QUANTITY_SCALE = 1_000_000_000L;

    /** Limit price scale factor: store prices as fixed-point with 6 decimal places. */
    public static final long PRICE_SCALE = 1_000_000L;

    /** Side wire codes — keep in sync with {@code com.balh.oms.domain.Side}. */
    public static final byte SIDE_BUY = 0;
    public static final byte SIDE_SELL = 1;

    /** Time-in-force wire codes — keep in sync with {@code com.balh.oms.domain.TimeInForce}. */
    public static final byte TIF_DAY = 0;
    public static final byte TIF_IOC = 1;
    public static final byte TIF_FOK = 2;
    public static final byte TIF_GTC = 3;

    /**
     * Map a wire {@code SIDE_*} byte to the canonical domain string ({@code "BUY"} / {@code "SELL"}).
     * Lives on the wire-format type so projector and {@link com.balh.oms.events.DomainEventEnvelopeCodec}
     * can build {@code orders} rows and domain envelopes from the same {@link OrderAdmittedEvent}
     * without each owning its own switch (Phase 4 Tier 2.5 phase D-3).
     *
     * @throws IllegalArgumentException on an unknown code (signals a wire format upgrade that
     *     was not yet propagated — fail loud rather than silently project an unknown side).
     */
    public static String sideName(byte sideCode) {
        return switch (sideCode) {
            case SIDE_BUY -> "BUY";
            case SIDE_SELL -> "SELL";
            default -> throw new IllegalArgumentException("unknown side code: " + sideCode);
        };
    }

    /**
     * Map a wire {@code TIF_*} byte to the canonical {@code time_in_force} string. See
     * {@link #sideName(byte)} for the rationale.
     */
    public static String timeInForceName(byte tif) {
        return switch (tif) {
            case TIF_DAY -> "DAY";
            case TIF_IOC -> "IOC";
            case TIF_FOK -> "FOK";
            case TIF_GTC -> "GTC";
            default -> throw new IllegalArgumentException("unknown time-in-force code: " + tif);
        };
    }

    public AcceptOrderCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(clientIdempotencyKey, "clientIdempotencyKey");
        Objects.requireNonNull(accountIdHash, "accountIdHash");
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
    }

    /**
     * Encode this command into {@code buffer} starting at {@code offset}.
     *
     * @return the number of bytes written.
     * @throws IllegalArgumentException if any string exceeds {@link OmsClusterWireFormat#MAX_STRING_BYTES}
     *         after UTF-8 encoding, or if total size would exceed
     *         {@link OmsClusterWireFormat#MAX_COMMAND_BYTES}.
     */
    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, correlationId);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, clientTimestampNanos);
        p += Long.BYTES;
        buffer.putLong(p, quantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, limitPriceScaledOrZero);
        p += Long.BYTES;
        buffer.putInt(p, shardId);
        p += Integer.BYTES;
        buffer.putByte(p++, side);
        buffer.putByte(p++, timeInForceCode);
        buffer.putByte(p++, (byte) (ledgerBalanceIdOrNull == null ? 0 : 1));
        buffer.putByte(p++, (byte) 0);

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, clientIdempotencyKey);
        p = writeString(buffer, p, accountIdHash);
        p = writeString(buffer, p, instrumentSymbol);
        if (ledgerBalanceIdOrNull != null) {
            p = writeString(buffer, p, ledgerBalanceIdOrNull);
        }

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "AcceptOrderCommand encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    /**
     * Decode an {@link AcceptOrderCommand} from {@code buffer} starting at
     * {@code offset}. The header is validated; the caller is responsible for
     * dispatching by {@link OmsClusterWireFormat#TYPE_ID_ACCEPT_ORDER}.
     */
    public static AcceptOrderCommand decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        int schema = buffer.getInt(offset + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET);
        if (schema != OmsClusterWireFormat.SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schema version " + schema);
        }
        long correlationId = buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);

        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        long clientTimestampNanos = buffer.getLong(p);
        p += Long.BYTES;
        long quantityScaled = buffer.getLong(p);
        p += Long.BYTES;
        long limitPriceScaledOrZero = buffer.getLong(p);
        p += Long.BYTES;
        int shardId = buffer.getInt(p);
        p += Integer.BYTES;
        byte side = buffer.getByte(p++);
        byte timeInForceCode = buffer.getByte(p++);
        byte hasLedgerBalanceId = buffer.getByte(p++);
        p++;

        String accountId = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String clientIdempotencyKey = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String accountIdHash = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String instrumentSymbol = readString(buffer, p);
        p += stringByteLenAt(buffer, p);
        String ledgerBalanceId = null;
        if (hasLedgerBalanceId == 1) {
            ledgerBalanceId = readString(buffer, p);
        }

        return new AcceptOrderCommand(
                correlationId,
                new UUID(msb, lsb),
                clientTimestampNanos,
                quantityScaled,
                limitPriceScaledOrZero,
                shardId,
                side,
                timeInForceCode,
                accountId,
                clientIdempotencyKey,
                accountIdHash,
                instrumentSymbol,
                ledgerBalanceId);
    }

    private static int writeString(MutableDirectBuffer buffer, int offset, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "string length " + bytes.length + " exceeds MAX_STRING_BYTES="
                            + OmsClusterWireFormat.MAX_STRING_BYTES);
        }
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + Integer.BYTES, bytes);
        return offset + Integer.BYTES + bytes.length;
    }

    private static String readString(DirectBuffer buffer, int offset) {
        int len = buffer.getInt(offset);
        if (len < 0 || len > OmsClusterWireFormat.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "string length " + len + " out of bounds [0, "
                            + OmsClusterWireFormat.MAX_STRING_BYTES + "]");
        }
        byte[] bytes = new byte[len];
        buffer.getBytes(offset + Integer.BYTES, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Number of bytes a string field occupies on the wire, read directly from the 4-byte length
     * prefix in {@code buffer} at {@code offset}. Used by {@link #decode} to advance the read
     * cursor between string fields without re-encoding the just-decoded {@link String} back to
     * UTF-8 bytes.
     *
     * <p>Phase 4 slice 4f of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}:
     * eliminates 4× redundant {@code byte[]} allocations per {@link AcceptOrderCommand} decode
     * (one per string field, ~176 B/op on production-shaped commands) by keeping the cursor
     * advance allocation-free. The byte length is identical to what the encoder wrote — see
     * {@link #writeString} where {@code buffer.putInt(offset, bytes.length)} is the same value
     * we read back here.
     */
    private static int stringByteLenAt(DirectBuffer buffer, int offset) {
        return Integer.BYTES + buffer.getInt(offset);
    }
}
