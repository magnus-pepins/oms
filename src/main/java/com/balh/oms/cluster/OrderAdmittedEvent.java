package com.balh.oms.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Projection event emitted by {@link OmsAdmissionClusteredService} on every <strong>fresh</strong> admission
 * (Phase 2 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}). Unlike {@link OrderAcceptedEvent}
 * — which travels per-session egress to the originating cluster client and only carries the
 * acceptance metadata needed to complete the HTTP request — this event travels the side projection
 * publication ({@link OmsClusterWireFormat#EVENTS_CHANNEL} / {@link OmsClusterWireFormat#EVENTS_STREAM_ID}),
 * is recorded by Aeron Archive on each cluster member, and carries enough order data for the Postgres
 * projector to materialise an {@code orders} row without consulting any other source.
 *
 * <p>Idempotent re-hits (the cluster service's idempotency index already knows this order) do
 * <strong>not</strong> emit a second {@code OrderAdmittedEvent}; the first emission's recording is the
 * authoritative projection signal. Cluster client egress still carries an
 * {@link OrderAcceptedEvent}{@code .duplicate=true} so the HTTP caller's correlation id completes.
 *
 * <p>Wire format (after the {@link OmsClusterWireFormat#HEADER_LENGTH} header):
 * <pre>
 *   offset 16  long  orderIdMsb
 *   offset 24  long  orderIdLsb
 *   offset 32  long  clientTimestampNanos        (received_at; epoch nanos, ingress wall-time)
 *   offset 40  long  acceptedAtMillis             (accepted_at; cluster timestamp in epoch millis,
 *                                                   per Aeron Cluster {@code ConsensusModule.Context.timeUnit()}
 *                                                   default {@code TimeUnit.MILLISECONDS}; not subtractable
 *                                                   from {@code clientTimestampNanos} without unit conversion)
 *   offset 48  long  quantityScaled               (1e9 fixed-point)
 *   offset 56  long  limitPriceScaledOrZero       (1e6 fixed-point; 0 = market)
 *   offset 64  int   shardId
 *   offset 68  int   version                      (always 0 today; future state changes bump it)
 *   offset 72  byte  side                         (AcceptOrderCommand.SIDE_*)
 *   offset 73  byte  timeInForceCode              (AcceptOrderCommand.TIF_*)
 *   offset 74  byte  hasLedgerBalanceId           (0 = no, 1 = yes)
 *   offset 75  byte  ordTypeCode                  (AcceptOrderCommand.ORD_TYPE_*)
 *   offset 76  string accountId
 *   offset N   string clientIdempotencyKey
 *   offset N   string accountIdHash
 *   offset N   string instrumentSymbol
 *   offset N   string ledgerBalanceId             (only when hasLedgerBalanceId == 1)
 * </pre>
 *
 * <p>The shape mirrors {@link AcceptOrderCommand} so projectors can map directly to {@code orders} columns
 * (see {@code OrdersRepository.INSERT_SQL}). {@code correlationId} is intentionally <strong>not</strong>
 * carried here — projectors consume by log position, not correlation, and the cluster-client per-session
 * egress still carries the correlation id on {@link OrderAcceptedEvent}.
 */
public record OrderAdmittedEvent(
        UUID orderId,
        long clientTimestampNanos,
        long acceptedAtMillis,
        long quantityScaled,
        long limitPriceScaledOrZero,
        int shardId,
        int version,
        byte side,
        byte timeInForceCode,
        byte ordTypeCode,
        String accountId,
        String clientIdempotencyKey,
        String accountIdHash,
        String instrumentSymbol,
        String ledgerBalanceIdOrNull,
        FixInIngressMetadata fixInIngressMetadataOrNull,
        String portfolioIdOrNull) {

    public OrderAdmittedEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(clientIdempotencyKey, "clientIdempotencyKey");
        Objects.requireNonNull(accountIdHash, "accountIdHash");
        Objects.requireNonNull(instrumentSymbol, "instrumentSymbol");
    }

    /**
     * Back-compat constructor for callers (mostly tests / projector unit harnesses) that
     * pre-date the Wed-demo {@code ordTypeCode} field. Defaults to
     * {@link AcceptOrderCommand#ORD_TYPE_MARKET} — same semantic the historical wire format
     * encoded when the slot was a hard-coded zero "reserved" byte. New code (including the
     * projector ↔ FIX-builder hot path) should always pass the explicit code.
     */
    public OrderAdmittedEvent(
            UUID orderId,
            long clientTimestampNanos,
            long acceptedAtMillis,
            long quantityScaled,
            long limitPriceScaledOrZero,
            int shardId,
            int version,
            byte side,
            byte timeInForceCode,
            String accountId,
            String clientIdempotencyKey,
            String accountIdHash,
            String instrumentSymbol,
            String ledgerBalanceIdOrNull) {
        this(orderId, clientTimestampNanos, acceptedAtMillis, quantityScaled, limitPriceScaledOrZero,
                shardId, version, side, timeInForceCode, AcceptOrderCommand.ORD_TYPE_MARKET,
                accountId, clientIdempotencyKey, accountIdHash, instrumentSymbol, ledgerBalanceIdOrNull,
                /* fixInIngressMetadataOrNull = */ null);
    }

    /** Back-compat constructor with explicit {@code ordTypeCode} and no FIX-in metadata. */
    public OrderAdmittedEvent(
            UUID orderId,
            long clientTimestampNanos,
            long acceptedAtMillis,
            long quantityScaled,
            long limitPriceScaledOrZero,
            int shardId,
            int version,
            byte side,
            byte timeInForceCode,
            byte ordTypeCode,
            String accountId,
            String clientIdempotencyKey,
            String accountIdHash,
            String instrumentSymbol,
            String ledgerBalanceIdOrNull) {
        this(orderId, clientTimestampNanos, acceptedAtMillis, quantityScaled, limitPriceScaledOrZero,
                shardId, version, side, timeInForceCode, ordTypeCode,
                accountId, clientIdempotencyKey, accountIdHash, instrumentSymbol, ledgerBalanceIdOrNull,
                /* fixInIngressMetadataOrNull = */ null);
    }

    /**
     * Back-compat constructor with explicit {@code ordTypeCode} and FIX-in metadata but no
     * {@code portfolioId} (the generic portfolio attribution tail is append-only). Defaults
     * {@code portfolioIdOrNull} to {@code null}.
     */
    public OrderAdmittedEvent(
            UUID orderId,
            long clientTimestampNanos,
            long acceptedAtMillis,
            long quantityScaled,
            long limitPriceScaledOrZero,
            int shardId,
            int version,
            byte side,
            byte timeInForceCode,
            byte ordTypeCode,
            String accountId,
            String clientIdempotencyKey,
            String accountIdHash,
            String instrumentSymbol,
            String ledgerBalanceIdOrNull,
            FixInIngressMetadata fixInIngressMetadataOrNull) {
        this(orderId, clientTimestampNanos, acceptedAtMillis, quantityScaled, limitPriceScaledOrZero,
                shardId, version, side, timeInForceCode, ordTypeCode,
                accountId, clientIdempotencyKey, accountIdHash, instrumentSymbol, ledgerBalanceIdOrNull,
                fixInIngressMetadataOrNull, /* portfolioIdOrNull = */ null);
    }

    /**
     * Convenience: build an event from an admitted command + the cluster's accepted-at timestamp.
     * Lives here (not in the service) so test code and the codec round-trip share one source of mapping.
     */
    public static OrderAdmittedEvent fromAdmittedCommand(AcceptOrderCommand cmd, long acceptedAtMillis, int version) {
        return new OrderAdmittedEvent(
                cmd.orderId(),
                cmd.clientTimestampNanos(),
                acceptedAtMillis,
                cmd.quantityScaled(),
                cmd.limitPriceScaledOrZero(),
                cmd.shardId(),
                version,
                cmd.side(),
                cmd.timeInForceCode(),
                cmd.ordTypeCode(),
                cmd.accountId(),
                cmd.clientIdempotencyKey(),
                cmd.accountIdHash(),
                cmd.instrumentSymbol(),
                cmd.ledgerBalanceIdOrNull(),
                cmd.fixInIngressMetadataOrNull(),
                cmd.portfolioIdOrNull());
    }

    public int encode(MutableDirectBuffer buffer, int offset) {
        int p = offset;
        buffer.putInt(p + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET, OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED);
        buffer.putInt(p + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET, OmsClusterWireFormat.SCHEMA_VERSION);
        buffer.putLong(p + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET, 0L);
        p += OmsClusterWireFormat.HEADER_LENGTH;

        buffer.putLong(p, orderId.getMostSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, orderId.getLeastSignificantBits());
        p += Long.BYTES;
        buffer.putLong(p, clientTimestampNanos);
        p += Long.BYTES;
        buffer.putLong(p, acceptedAtMillis);
        p += Long.BYTES;
        buffer.putLong(p, quantityScaled);
        p += Long.BYTES;
        buffer.putLong(p, limitPriceScaledOrZero);
        p += Long.BYTES;
        buffer.putInt(p, shardId);
        p += Integer.BYTES;
        buffer.putInt(p, version);
        p += Integer.BYTES;
        buffer.putByte(p++, side);
        buffer.putByte(p++, timeInForceCode);
        buffer.putByte(p++, (byte) (ledgerBalanceIdOrNull == null ? 0 : 1));
        buffer.putByte(p++, ordTypeCode);

        p = writeString(buffer, p, accountId);
        p = writeString(buffer, p, clientIdempotencyKey);
        p = writeString(buffer, p, accountIdHash);
        p = writeString(buffer, p, instrumentSymbol);
        if (ledgerBalanceIdOrNull != null) {
            p = writeString(buffer, p, ledgerBalanceIdOrNull);
        }
        if (fixInIngressMetadataOrNull != null) {
            p += FixInIngressMetadata.writeFixInTail(buffer, p, fixInIngressMetadataOrNull);
        }
        if (portfolioIdOrNull != null) {
            buffer.putByte(p++, FixInIngressMetadata.SECTION_PORTFOLIO_ID);
            p = writeString(buffer, p, portfolioIdOrNull);
        }

        int written = p - offset;
        if (written > OmsClusterWireFormat.MAX_COMMAND_BYTES) {
            throw new IllegalArgumentException(
                    "OrderAdmittedEvent encoded length " + written
                            + " exceeds MAX_COMMAND_BYTES=" + OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
        return written;
    }

    public static OrderAdmittedEvent decode(DirectBuffer buffer, int offset, int length) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            throw new IllegalArgumentException("buffer too short for header: " + length);
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
            throw new IllegalArgumentException("unexpected typeId " + typeId);
        }
        int schema = buffer.getInt(offset + OmsClusterWireFormat.HEADER_SCHEMA_VERSION_OFFSET);
        if (schema != OmsClusterWireFormat.SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schema version " + schema);
        }

        int p = offset + OmsClusterWireFormat.HEADER_LENGTH;
        long msb = buffer.getLong(p);
        p += Long.BYTES;
        long lsb = buffer.getLong(p);
        p += Long.BYTES;
        long clientTimestampNanos = buffer.getLong(p);
        p += Long.BYTES;
        long acceptedAtMillis = buffer.getLong(p);
        p += Long.BYTES;
        long quantityScaled = buffer.getLong(p);
        p += Long.BYTES;
        long limitPriceScaledOrZero = buffer.getLong(p);
        p += Long.BYTES;
        int shardId = buffer.getInt(p);
        p += Integer.BYTES;
        int version = buffer.getInt(p);
        p += Integer.BYTES;
        byte side = buffer.getByte(p++);
        byte timeInForceCode = buffer.getByte(p++);
        byte hasLedgerBalanceId = buffer.getByte(p++);
        // Pre-V33 events wrote 0 (reserved); decoded as ORD_TYPE_MARKET, matching the legacy
        // semantic ("MARKET unless limit_price > 0"). See AcceptOrderCommand.ORD_TYPE_*.
        byte ordTypeCode = buffer.getByte(p++);

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
            p += stringByteLenAt(buffer, p);
        }
        // Generic optional tail: a sequence of [sectionByte][payload] blocks read until end-of-frame.
        // Absent on legacy / REST-without-portfolio frames (loop body never runs). Append-only —
        // see FixInIngressMetadata.SECTION_PORTFOLIO_ID; SCHEMA_VERSION is intentionally not bumped.
        FixInIngressMetadata fixInIngressMetadata = null;
        String portfolioId = null;
        int end = offset + length;
        while (p < end) {
            byte sectionType = buffer.getByte(p);
            if (sectionType == FixInIngressMetadata.INGRESS_TYPE_NONE) {
                p++;
                break;
            } else if (sectionType == FixInIngressMetadata.INGRESS_TYPE_FIX_IN) {
                fixInIngressMetadata = FixInIngressMetadata.readFixInTailIfPresent(buffer, p, end);
                p += FixInIngressMetadata.fixInTailByteLength(buffer, p);
            } else if (sectionType == FixInIngressMetadata.SECTION_PORTFOLIO_ID) {
                p++;
                portfolioId = readString(buffer, p);
                p += stringByteLenAt(buffer, p);
            } else {
                throw new IllegalArgumentException("unsupported tail section byte on wire: " + sectionType);
            }
        }

        return new OrderAdmittedEvent(
                new UUID(msb, lsb),
                clientTimestampNanos,
                acceptedAtMillis,
                quantityScaled,
                limitPriceScaledOrZero,
                shardId,
                version,
                side,
                timeInForceCode,
                ordTypeCode,
                accountId,
                clientIdempotencyKey,
                accountIdHash,
                instrumentSymbol,
                ledgerBalanceId,
                fixInIngressMetadata,
                portfolioId);
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
     * prefix in {@code buffer} at {@code offset}. See {@code AcceptOrderCommand#stringByteLenAt}
     * for the slice-4f rationale (eliminates redundant {@code byte[]} allocations on decode).
     */
    private static int stringByteLenAt(DirectBuffer buffer, int offset) {
        return Integer.BYTES + buffer.getInt(offset);
    }
}
