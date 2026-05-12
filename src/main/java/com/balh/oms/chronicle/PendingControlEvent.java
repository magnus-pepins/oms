package com.balh.oms.chronicle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * In-memory domain view of a control-plane event. Persisted in {@code control_outbox.payload} as JSONB wrapping
 * base64 {@link com.balh.oms.proto.control.v1.ControlPendingEvent} (see {@link ControlChroniclePayloadCodec}); appended
 * to Chronicle as {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf (e.g. {@link ControlChronicleEventTypes#ORDER_ACCEPTED},
 * {@link ControlChronicleEventTypes#CONTROL_PIPELINE_TELEMETRY}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public record PendingControlEvent(
        String type,
        UUID orderId,
        int orderVersion,
        int shardId,
        String accountIdHash,
        Instant orderTimestamp,
        Instant enqueuedAt,
        Optional<Instant> chronicleMaterializedAt,
        List<ControlTelemetryHop> telemetryHops
) {
    public PendingControlEvent {
        Objects.requireNonNull(telemetryHops, "telemetryHops");
        telemetryHops = List.copyOf(telemetryHops);
    }

    /** Ingress / tests: no reconciler or tail telemetry on the domain row yet. */
    public PendingControlEvent(
            String type,
            UUID orderId,
            int orderVersion,
            int shardId,
            String accountIdHash,
            Instant orderTimestamp,
            Instant enqueuedAt) {
        this(type, orderId, orderVersion, shardId, accountIdHash, orderTimestamp, enqueuedAt, Optional.empty(), List.of());
    }
}
