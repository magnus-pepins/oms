package com.balh.oms.cluster;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Aeron {@link ClusteredService} hosting the OMS order-admission state machine.
 *
 * <p>Per ADR 0001 ({@code docs/adr/0001-aeron-cluster-substrate.md}), this is
 * the source of truth for "is this order admitted". Postgres becomes a
 * downstream projection driven by the events this service emits.
 *
 * <h3>Determinism rules (enforced by review and by future lint)</h3>
 * <ul>
 *   <li>No {@code Instant.now()}, {@code System.currentTimeMillis()},
 *       {@code System.nanoTime()}, {@code UUID.randomUUID()},
 *       {@code java.util.Random}. Time and ids come from command payloads or
 *       the cluster-supplied {@code timestamp} parameter.</li>
 *   <li>No external I/O (no Postgres, no HTTP, no FIX, no logging at INFO+
 *       inside the hot path). External effects happen at the edge.</li>
 *   <li>No reflection, no Spring annotations on this class. Wired by hand
 *       from {@code AeronClusterContext}.</li>
 * </ul>
 *
 * <h3>State (in-memory)</h3>
 * <ul>
 *   <li>{@code idempotencyIndex}: {@code (accountId, clientIdempotencyKey) ->
 *       AdmittedOrder}. Used to short-circuit duplicate {@link AcceptOrderCommand}s
 *       and emit a {@link OrderAcceptedEvent} with {@code duplicate=true}.</li>
 *   <li>{@code orderIndex}: {@code orderId -> AdmittedOrder}. Used by later
 *       phases that mutate accepted orders (cancel, fill, etc.). Today only
 *       used to keep both indexes consistent.</li>
 * </ul>
 *
 * <h3>Snapshot format (transitional)</h3>
 *
 * <p>This first scaffold uses a hand-rolled binary snapshot fragment. SBE
 * replaces it before any cross-language consumer attaches (see ADR 0001). The
 * snapshot is a single fragment with header
 * {@link #SNAPSHOT_MAGIC} + schema version + order count, followed by
 * {@code count} {@link AdmittedOrder} entries.
 *
 * <p>Snapshot size scales linearly with admitted order count; the buffer below
 * is {@link ExpandableArrayBuffer} so large snapshots grow on demand. Phase 2+
 * fragments the snapshot across multiple Aeron messages once realistic order
 * cardinalities land.
 */
public class OmsAdmissionClusteredService implements ClusteredService {

    private static final Logger log = LoggerFactory.getLogger(OmsAdmissionClusteredService.class);

    /** Magic number identifying an OMS admission snapshot fragment ("OMSA" in ASCII). */
    static final int SNAPSHOT_MAGIC = 0x4F4D5341;

    /** Snapshot schema version. Bumped only on incompatible state changes. */
    static final int SNAPSHOT_SCHEMA_VERSION = 1;

    /** Initial buffer capacity for command processing. Grown on demand. */
    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    /** Initial map capacity (per-shard). Avoids early resizes for warm-up loads. */
    private static final int INITIAL_INDEX_CAPACITY = 4096;

    private final Map<IdempotencyKey, AdmittedOrder> idempotencyIndex =
            new HashMap<>(INITIAL_INDEX_CAPACITY);
    private final Map<UUID, AdmittedOrder> orderIndex = new HashMap<>(INITIAL_INDEX_CAPACITY);

    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);

    /**
     * Buffer for {@link OrderAdmittedEvent} payloads written to {@link #eventsPublication}. Held separately
     * from {@link #egressBuffer} because the projection event is larger than the per-session
     * {@link OrderAcceptedEvent} and growing the egress buffer to the same size would waste memory on every
     * cluster session offer.
     */
    private final ExpandableArrayBuffer eventsBuffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);

    private Cluster cluster;

    /**
     * Side publication carrying {@link OrderAdmittedEvent}s for the Postgres projector (Phase 2).
     *
     * <p>Created on {@link #onStart(Cluster, Image)} via {@code cluster.aeron().addExclusivePublication}.
     * The corresponding Aeron Archive recording is started by the cluster bootstrap
     * ({@code OmsClusterNodeBootstrap.startEventsRecording}) <em>before</em> the cluster comes up, so the
     * Archive sees the publication as it appears and records every event from position 0 — which means
     * the projector's cursor advances along the same byte stream the Archive recorded.
     *
     * <p>Lifecycle: opened on {@code onStart}, closed on {@code onTerminate}. Replay (after a snapshot
     * load) does <em>not</em> re-emit prior events on this publication — the recording already holds
     * them; the projector reads from the recording, not from the live publication.
     */
    private ExclusivePublication eventsPublication;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        // Determinism note: addExclusivePublication is a deterministic side effect — same cluster
        // configuration produces the same publication on every member / replay. The publication is the
        // bridge to the Archive recording, which is the durable projection signal.
        this.eventsPublication = cluster.aeron().addExclusivePublication(
                OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID);
        log.info(
                "OmsAdmissionClusteredService started; orders={}, role={}, eventsPub={}/{}",
                orderIndex.size(),
                cluster.role(),
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // No per-session state today. Cluster client sessions are stateless; idempotency lives in the state machine.
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        // No per-session state to clean up. See onSessionOpen.
    }

    @Override
    public void onSessionMessage(
            ClientSession session,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header) {
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            // Malformed; ignore. Logging here would break determinism on replay if log writes throw.
            return;
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        switch (typeId) {
            case OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER ->
                    applyAcceptOrder(session, timestamp, buffer, offset, length);
            default -> {
                // Unknown command — silently ignored. A real reject would require an event;
                // adding an UnknownCommandRejected event is a Phase 2 concern.
            }
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // No timers scheduled today. Slice that adds market-hours / timeout cancels will use them.
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);
        int p = 0;
        buffer.putInt(p, SNAPSHOT_MAGIC);
        p += Integer.BYTES;
        buffer.putInt(p, SNAPSHOT_SCHEMA_VERSION);
        p += Integer.BYTES;
        buffer.putInt(p, orderIndex.size());
        p += Integer.BYTES;
        for (AdmittedOrder o : orderIndex.values()) {
            p = o.encode(buffer, p);
        }
        long pos;
        while ((pos = snapshotPublication.offer(buffer, 0, p)) < 0L) {
            // BACK_PRESSURED / NOT_CONNECTED / ADMIN_ACTION — retry.
            // Aeron's standard snapshot publish loop. No external IO, no allocation here.
            if (pos == ExclusivePublication.CLOSED || pos == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("snapshot publication closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void loadSnapshot(Image snapshotImage) {
        SnapshotLoader loader = new SnapshotLoader();
        while (!snapshotImage.isEndOfStream()) {
            int frags = snapshotImage.poll(loader, 1);
            if (frags == 0) {
                Thread.yield();
            }
        }
        log.info("loaded admission snapshot: orders={}", orderIndex.size());
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        log.info("OmsAdmissionClusteredService role change -> {}", newRole);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("OmsAdmissionClusteredService terminating; orders={}", orderIndex.size());
        CloseHelper.quietClose(eventsPublication);
        eventsPublication = null;
    }

    // ------------------------------------------------------------------------
    // Command handlers (deterministic apply)
    // ------------------------------------------------------------------------

    private void applyAcceptOrder(
            ClientSession session, long clusterTimestampNs, DirectBuffer buffer, int offset, int length) {
        AcceptOrderCommand cmd = AcceptOrderCommand.decode(buffer, offset, length);

        IdempotencyKey key = new IdempotencyKey(cmd.accountId(), cmd.clientIdempotencyKey());
        AdmittedOrder existing = idempotencyIndex.get(key);
        if (existing != null) {
            // Idempotent re-hit. The projector already saw the first emission; do not re-emit on the side
            // publication. Per-session egress still tells the originating client {duplicate=true}.
            emitAccepted(session, cmd.correlationId(), existing, clusterTimestampNs, true);
            return;
        }

        AdmittedOrder admitted = new AdmittedOrder(
                cmd.orderId(),
                cmd.accountId(),
                cmd.clientIdempotencyKey(),
                cmd.accountIdHash(),
                cmd.instrumentSymbol(),
                cmd.side(),
                cmd.quantityScaled(),
                cmd.limitPriceScaledOrZero(),
                cmd.timeInForceCode(),
                cmd.ledgerBalanceIdOrNull(),
                /* version = */ 0,
                clusterTimestampNs);
        idempotencyIndex.put(key, admitted);
        orderIndex.put(admitted.orderId(), admitted);

        emitAdmitted(cmd, clusterTimestampNs, admitted.version());
        emitAccepted(session, cmd.correlationId(), admitted, clusterTimestampNs, false);
    }

    private void emitAdmitted(AcceptOrderCommand cmd, long acceptedAtNanos, int version) {
        if (eventsPublication == null) {
            // Defensive: cluster is shutting down. Not expected on the apply path; the consensus module
            // halts message delivery before onTerminate, but we guard so a late frame does not NPE.
            return;
        }
        OrderAdmittedEvent ev = OrderAdmittedEvent.fromAdmittedCommand(cmd, acceptedAtNanos, version);
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED / NOT_CONNECTED is normal during steady-state and during projector reconnect.
            // The Archive subscribes to this publication on the cluster member and is always present.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitAccepted(
            ClientSession session,
            long correlationId,
            AdmittedOrder admitted,
            long acceptedAtNanos,
            boolean duplicate) {
        OrderAcceptedEvent ev = new OrderAcceptedEvent(
                correlationId, admitted.orderId(), admitted.version(), duplicate, acceptedAtNanos);
        int len = ev.encode(egressBuffer, 0);
        long pos;
        while ((pos = session.offer(egressBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED is normal under load — retry. Closed/terminal positions throw.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("session.offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    // ------------------------------------------------------------------------
    // Test / projection helpers (read-only views of state)
    // ------------------------------------------------------------------------

    /** Visible for tests; do not mutate the returned reference. */
    public AdmittedOrder lookupByIdempotency(String accountId, String clientIdempotencyKey) {
        return idempotencyIndex.get(new IdempotencyKey(accountId, clientIdempotencyKey));
    }

    /** Visible for tests; do not mutate the returned reference. */
    public AdmittedOrder lookupByOrderId(UUID orderId) {
        return orderIndex.get(orderId);
    }

    /** Visible for tests. Total admitted orders in the state machine. */
    public int admittedOrderCount() {
        return orderIndex.size();
    }

    private final class SnapshotLoader implements io.aeron.logbuffer.FragmentHandler {
        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            int p = offset;
            int magic = buffer.getInt(p);
            p += Integer.BYTES;
            if (magic != SNAPSHOT_MAGIC) {
                throw new IllegalStateException("snapshot magic mismatch: 0x" + Integer.toHexString(magic));
            }
            int schemaVersion = buffer.getInt(p);
            p += Integer.BYTES;
            if (schemaVersion != SNAPSHOT_SCHEMA_VERSION) {
                throw new IllegalStateException("unsupported snapshot schema version " + schemaVersion);
            }
            int count = buffer.getInt(p);
            p += Integer.BYTES;
            for (int i = 0; i < count; i++) {
                AdmittedOrder o = AdmittedOrder.decode(buffer, p);
                p += o.encodedLength();
                idempotencyIndex.put(new IdempotencyKey(o.accountId(), o.clientIdempotencyKey()), o);
                orderIndex.put(o.orderId(), o);
            }
        }
    }

    /** Composite key for the idempotency index. Records auto-generate equals/hashCode. */
    record IdempotencyKey(String accountId, String clientIdempotencyKey) {}

    /**
     * Snapshot of an admitted order held in the state machine.
     *
     * <p>This is intentionally a small, dense record. It is the source of truth
     * for "what does the cluster know about this order"; Postgres is a
     * downstream projection of these.
     */
    public record AdmittedOrder(
            UUID orderId,
            String accountId,
            String clientIdempotencyKey,
            String accountIdHash,
            String instrumentSymbol,
            byte side,
            long quantityScaled,
            long limitPriceScaledOrZero,
            byte timeInForceCode,
            String ledgerBalanceIdOrNull,
            int version,
            long acceptedAtNanos) {

        int encode(MutableDirectBuffer buffer, int offset) {
            int p = offset;
            buffer.putLong(p, orderId.getMostSignificantBits());
            p += Long.BYTES;
            buffer.putLong(p, orderId.getLeastSignificantBits());
            p += Long.BYTES;
            buffer.putInt(p, version);
            p += Integer.BYTES;
            buffer.putLong(p, acceptedAtNanos);
            p += Long.BYTES;
            buffer.putLong(p, quantityScaled);
            p += Long.BYTES;
            buffer.putLong(p, limitPriceScaledOrZero);
            p += Long.BYTES;
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
            return p;
        }

        static AdmittedOrder decode(DirectBuffer buffer, int offset) {
            int p = offset;
            long msb = buffer.getLong(p);
            p += Long.BYTES;
            long lsb = buffer.getLong(p);
            p += Long.BYTES;
            int version = buffer.getInt(p);
            p += Integer.BYTES;
            long acceptedAtNanos = buffer.getLong(p);
            p += Long.BYTES;
            long quantityScaled = buffer.getLong(p);
            p += Long.BYTES;
            long limitPriceScaledOrZero = buffer.getLong(p);
            p += Long.BYTES;
            byte side = buffer.getByte(p++);
            byte timeInForceCode = buffer.getByte(p++);
            byte hasLedgerBalanceId = buffer.getByte(p++);
            p++;
            String accountId = readString(buffer, p);
            p += stringByteLen(accountId);
            String clientIdempotencyKey = readString(buffer, p);
            p += stringByteLen(clientIdempotencyKey);
            String accountIdHash = readString(buffer, p);
            p += stringByteLen(accountIdHash);
            String instrumentSymbol = readString(buffer, p);
            p += stringByteLen(instrumentSymbol);
            String ledgerBalanceId = null;
            if (hasLedgerBalanceId == 1) {
                ledgerBalanceId = readString(buffer, p);
            }
            return new AdmittedOrder(
                    new UUID(msb, lsb),
                    accountId,
                    clientIdempotencyKey,
                    accountIdHash,
                    instrumentSymbol,
                    side,
                    quantityScaled,
                    limitPriceScaledOrZero,
                    timeInForceCode,
                    ledgerBalanceId,
                    version,
                    acceptedAtNanos);
        }

        int encodedLength() {
            int p = 0;
            p += Long.BYTES * 5;
            p += Integer.BYTES;
            p += 4;
            p += stringByteLen(accountId);
            p += stringByteLen(clientIdempotencyKey);
            p += stringByteLen(accountIdHash);
            p += stringByteLen(instrumentSymbol);
            if (ledgerBalanceIdOrNull != null) {
                p += stringByteLen(ledgerBalanceIdOrNull);
            }
            return p;
        }

        private static int writeString(MutableDirectBuffer buffer, int offset, String s) {
            byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.putInt(offset, bytes.length);
            buffer.putBytes(offset + Integer.BYTES, bytes);
            return offset + Integer.BYTES + bytes.length;
        }

        private static String readString(DirectBuffer buffer, int offset) {
            int len = buffer.getInt(offset);
            byte[] bytes = new byte[len];
            buffer.getBytes(offset + Integer.BYTES, bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        private static int stringByteLen(String s) {
            return Integer.BYTES + s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }
}
