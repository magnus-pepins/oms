package com.balh.oms.cluster;

/**
 * Wire-format constants for OMS cluster commands and events.
 *
 * <p>The wire format is intentionally simple binary (length-prefixed strings,
 * fixed-width primitives, little-endian) to keep the first scaffolding PRs
 * narrow. Per ADR 0001 ({@code docs/adr/0001-aeron-cluster-substrate.md}),
 * SBE (Simple Binary Encoding) replaces this hand-rolled format before any
 * cross-language consumer (e.g. a future C++ matching engine module) attaches.
 *
 * <p>Type IDs are stable: never reuse a number for a different type. Add new
 * commands / events with new IDs.
 *
 * <p>All multi-byte integer reads/writes use little-endian via Agrona's
 * {@code DirectBuffer.getXxx(offset)} / {@code MutableDirectBuffer.putXxx(offset)}
 * default byte order, which matches Aeron's wire format. Do not switch byte
 * order — the cluster log durability depends on consistent encoding across
 * versions.
 */
public final class OmsClusterWireFormat {

    private OmsClusterWireFormat() {}

    /**
     * Schema version embedded in every command and event header. Bumped when
     * the wire format changes in a way that is not append-only-compatible.
     * Older versions remain decodable forever (cluster snapshots and log replay
     * cannot be rewritten in production).
     */
    public static final int SCHEMA_VERSION = 1;

    /**
     * Header layout (every command and event starts with this):
     *
     * <pre>
     *   offset 0  int  typeId
     *   offset 4  int  schemaVersion
     *   offset 8  long correlationId   (echoed back in response events)
     *   offset 16 ...  type-specific payload
     * </pre>
     */
    public static final int HEADER_TYPE_ID_OFFSET = 0;
    public static final int HEADER_SCHEMA_VERSION_OFFSET = 4;
    public static final int HEADER_CORRELATION_ID_OFFSET = 8;
    public static final int HEADER_LENGTH = 16;

    /** Maximum length (in bytes after UTF-8 encoding) of any string field on the wire. */
    public static final int MAX_STRING_BYTES = 256;

    /**
     * Maximum total command size on the wire. Bounds the cluster ingress buffer
     * and prevents pathological allocations from a malformed client. Tune via
     * {@code oms.aeron.cluster.max-command-bytes} once the cluster client lands.
     */
    public static final int MAX_COMMAND_BYTES = 4096;

    /**
     * Maximum batched-command size on the wire. Larger than
     * {@link #MAX_COMMAND_BYTES} because a {@link #TYPE_ID_BATCH_ACCEPT_ORDER}
     * frame inlines many {@link AcceptOrderCommand} bodies (Phase 4 Tier 2.5
     * phase D-6). Sized for {@code maxBatchSize=64 × ~360-byte per-cmd} plus
     * a small header — well under Aeron's 16 KB term-buffer page so single-frame
     * delivery is preserved (no MDS fragmentation).
     */
    public static final int MAX_BATCH_COMMAND_BYTES = 32_768;

    // ---- Command type IDs (1..999) ----

    /** {@link AcceptOrderCommand}. */
    public static final int TYPE_ID_ACCEPT_ORDER = 1;

    /**
     * {@link ApplyExecutionReportCommand}. Phase 3 of
     * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: the cluster service is the
     * source of truth for execution-report state transitions (fill / partial / cancel / venue reject).
     */
    public static final int TYPE_ID_APPLY_EXECUTION_REPORT = 2;

    /**
     * {@link BatchAcceptOrderCommand}. Phase 4 Tier 2.5 phase D-6: client packs N
     * {@link AcceptOrderCommand} bodies into one Aeron cluster message so the
     * cluster's single-leader admit thread amortises per-message framing /
     * consensus overhead across N admits. The cluster service decodes the batch
     * and dispatches inner {@link AcceptOrderCommand}s through the same
     * {@link OmsAdmissionClusteredService#applyAcceptOrder applyAcceptOrder} path
     * — each inner command keeps its own {@code correlationId} so the
     * {@link OmsClusterIngressClient} egress demux is identical to the unbatched
     * path (replies arrive as N {@link OrderAcceptedEvent}s / {@link OrderRejectedEvent}s).
     */
    public static final int TYPE_ID_BATCH_ACCEPT_ORDER = 4;

    /**
     * {@link CancelOrderCommand}. Phase 4 slice 4p: OMS-initiated cancel issued by the
     * {@code LedgerInflightHoldFailureCompensator} when a buying-power hold fails on the async
     * outbox path (Tier 2.5). Distinct from {@link #TYPE_ID_APPLY_EXECUTION_REPORT} with
     * {@code execTypeCode=EXEC_TYPE_CANCEL}, which represents a venue-side cancel: this command
     * never touched a venue, so the cluster service does not insert an {@code executions} row
     * (downstream projector branch on {@link #TYPE_ID_ORDER_CANCEL_APPLIED}). The command is
     * idempotent in the cluster: replay or a second compensator hit on the same {@code orderId}
     * sees the order is terminal and silently no-ops.
     */
    public static final int TYPE_ID_CANCEL_ORDER = 3;

    /**
     * {@link RequestCancelOrderCommand}. User-initiated cancel routed to the broker via FIX 35=F.
     * <strong>Distinct from {@link #TYPE_ID_CANCEL_ORDER}</strong>: the latter is an internal
     * inflight-failure cancel that never touches a venue and immediately CANCELS the order; this
     * one is the customer-/desk-initiated cancel that goes to the broker and only changes status
     * once the broker's ER (ET=4 CANCELED) lands. The cluster service emits an
     * {@link #TYPE_ID_ORDER_CANCEL_REQUESTED} event on the side publication; oms-fix-egress
     * consumes that event and sends 35=F to the broker. No status mutation happens here — the
     * order stays WORKING / PARTIALLY_FILLED until the ER arrives.
     */
    public static final int TYPE_ID_REQUEST_CANCEL_ORDER = 5;

    /**
     * {@link RequestReplaceOrderCommand}. User-initiated modify (qty + limit price) routed to
     * the broker via FIX 35=G OrderCancelReplaceRequest. Cluster validates the order is live,
     * dedupes on {@code (orderId, clientRequestKey)} to make HTTP retries idempotent, and emits
     * an {@link #TYPE_ID_ORDER_REPLACE_REQUESTED} for oms-fix-egress. Status / qty / price stay
     * unchanged on the apply path; the broker's ER (ET=5 REPLACED) carries the authoritative new
     * values and {@link #TYPE_ID_APPLY_EXECUTION_REPORT} with {@code execTypeCode=EXEC_TYPE_REPLACE}
     * mutates the order in place.
     */
    public static final int TYPE_ID_REQUEST_REPLACE_ORDER = 6;

    /**
     * {@link ApplyVenueResolutionCommand}. Phase B prediction-market resolution: transitions open
     * orders on a contract to {@code RESOLVED} and emits {@link VenueResolutionAppliedEvent}.
     */
    public static final int TYPE_ID_APPLY_VENUE_RESOLUTION = 7;

    // ---- Event type IDs (1000..1999) ----

    /** {@link OrderAcceptedEvent}. */
    public static final int TYPE_ID_ORDER_ACCEPTED = 1000;

    /** {@link OrderRejectedEvent}. */
    public static final int TYPE_ID_ORDER_REJECTED = 1001;

    /** {@link OrderAdmittedEvent}. Phase 2 projection event. */
    public static final int TYPE_ID_ORDER_ADMITTED = 1002;

    /**
     * {@link ExecutionAppliedEvent}. Phase 3 projection event emitted to the side publication after the
     * cluster's deterministic state machine applies an {@link ApplyExecutionReportCommand}. Slice 3e folds
     * {@code ExecutionReportApplier}'s Postgres effects (executions / orders / control_decisions) into the
     * projector by consuming this event.
     */
    public static final int TYPE_ID_EXECUTION_APPLIED = 1003;

    /**
     * {@link OrderCancelAppliedEvent}. Phase 4 slice 4p projection event emitted to the side publication
     * after the cluster's deterministic state machine applies a {@link #TYPE_ID_CANCEL_ORDER}. Distinct
     * from {@link #TYPE_ID_EXECUTION_APPLIED} with {@code execTypeCode=EXEC_TYPE_CANCEL} so the projector
     * can apply the OMS-initiated path with no {@code executions} row insert (the cancel never touched a
     * venue) and a different domain event envelope shape (no {@code venueId} / {@code venueExecRef}).
     */
    public static final int TYPE_ID_ORDER_CANCEL_APPLIED = 1004;

    /**
     * {@link OrderCancelRequestedEvent}. Wed-demo addition: emitted by the cluster after a
     * {@link #TYPE_ID_REQUEST_CANCEL_ORDER} command admits. The oms-fix-egress JVM consumes this
     * from the side publication and sends 35=F OrderCancelRequest to the broker. The projector
     * also writes a {@code domain_event_outbox} row carrying an {@code OrderCancelRequested}
     * envelope so the trading-desk and customer-frontend can show a transient "cancel
     * requested" badge while the broker round-trips.
     */
    public static final int TYPE_ID_ORDER_CANCEL_REQUESTED = 1005;

    /**
     * {@link OrderReplaceRequestedEvent}. Same role as {@link #TYPE_ID_ORDER_CANCEL_REQUESTED} but
     * for the modify path: oms-fix-egress builds 35=G with the new qty + price, projector emits a
     * domain envelope, UIs render the pending modify until the broker's ER ET=5 lands.
     *
     * <p>The matching 35=9 reject paths reuse {@link #TYPE_ID_EXECUTION_APPLIED} carrying
     * {@code execTypeCode=EXEC_TYPE_CANCEL_REJECT} or {@code EXEC_TYPE_REPLACE_REJECT}; the
     * projector branches on the discriminator to skip the {@code orders} status mutation and
     * write only the {@code OrderCancelRejected} / {@code OrderReplaceRejected} domain envelope
     * to {@code domain_event_outbox}.
     */
    public static final int TYPE_ID_ORDER_REPLACE_REQUESTED = 1006;

    /**
     * {@link VenueResolutionAppliedEvent}. Phase B projection event after
     * {@link ApplyVenueResolutionCommand} applies on the cluster.
     */
    public static final int TYPE_ID_VENUE_RESOLUTION_APPLIED = 1007;

    /** Binary contract outcome wire codes (mirrors balh-venue {@code VenueClusterWireFormat}). */
    public static final byte OUTCOME_YES = 1;
    public static final byte OUTCOME_NO = 2;

    // ---- Cluster→projector event stream (Phase 2) ----

    /**
     * Aeron channel for the projection event stream emitted by
     * {@link OmsAdmissionClusteredService}. IPC is correct for in-process tests and for production where the
     * Aeron Archive on the cluster node records the publication locally; remote projectors consume the stream
     * via {@code AeronArchive} replay over the cluster's archive control channel, not by subscribing to this
     * channel directly.
     *
     * <p>Phase 5 may add a UDP variant for live multicast fan-out to co-located stateless consumers, but that
     * does not change the recorded-channel contract; the recording is always the source of truth for
     * projector cursor.
     */
    public static final String EVENTS_CHANNEL = "aeron:ipc?term-length=64k";

    /**
     * Aeron stream id for the projection event stream. Chosen well outside the range Aeron Cluster uses for
     * its internal streams (cluster log, snapshot, ingress, egress are 100..199 by default in
     * {@code AeronCluster.Configuration}). Not configurable by env — projector and cluster both compile
     * against this constant.
     */
    public static final int EVENTS_STREAM_ID = 2000;
}
