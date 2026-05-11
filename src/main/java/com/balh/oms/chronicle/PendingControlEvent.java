package com.balh.oms.chronicle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * In-memory domain view of a control-plane event. Persisted in {@code control_outbox.payload} as JSONB wrapping
 * base64 {@link com.balh.oms.proto.control.v1.ControlPendingEvent} (see {@link ControlChroniclePayloadCodec}); appended
 * to Chronicle as {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf. Legacy rows/excerpts used flat
 * JSON of this record and remain readable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PendingControlEvent(
        String type,
        UUID orderId,
        int orderVersion,
        int shardId,
        String accountIdHash,
        Instant orderTimestamp,
        Instant enqueuedAt
) {}
