package com.balh.oms.chronicle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Control-plane payload written to {@code control_outbox.payload} and replayed
 * onto Chronicle by the reconciler as <strong>UTF-8 JSON</strong> of this record
 * (Jackson {@code ObjectMapper} in {@link ChronicleControlTailReader}) — not Protocol Buffers today.
 * Designed to be self-describing so the tailer can apply CAS updates without re-reading anything
 * other than the orders row at {@link #orderVersion()}.
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
