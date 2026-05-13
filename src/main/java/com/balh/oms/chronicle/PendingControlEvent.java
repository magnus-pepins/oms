package com.balh.oms.chronicle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

/**
 * In-memory domain view of a control-plane admission event.
 *
 * <p>Once carried the Chronicle wire-format coupling (proto envelope, telemetry hops, materialized-at
 * stamps); after Phase 3 slice 3g of the Aeron Cluster substrate plan deleted the chronicle module,
 * this is just the input record {@link com.balh.oms.tailer.OrderControlAdmission#persistAdmission}
 * needs from {@link com.balh.oms.cluster.OrderAdmittedEvent}. The package name is legacy — the type
 * itself is no longer chronicle-bound; renaming it is a follow-on cleanup.
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
        Instant enqueuedAt
) {
}
