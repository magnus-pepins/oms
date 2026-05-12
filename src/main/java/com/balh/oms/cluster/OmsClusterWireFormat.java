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

    // ---- Command type IDs (1..999) ----

    /** {@link AcceptOrderCommand}. */
    public static final int TYPE_ID_ACCEPT_ORDER = 1;

    // ---- Event type IDs (1000..1999) ----

    /** {@link OrderAcceptedEvent}. */
    public static final int TYPE_ID_ORDER_ACCEPTED = 1000;

    /** {@link OrderRejectedEvent}. */
    public static final int TYPE_ID_ORDER_REJECTED = 1001;
}
