package com.balh.oms.returnpath;

import java.time.Instant;
import java.util.UUID;

/**
 * Venue or broker rejected the order (FIX {@code ExecutionReport} {@code ExecType=Rejected}
 * or {@code OrderCancelReject}), mapped to {@code OrderRejected} after CAS to {@code REJECTED}.
 */
public record ExecutionVenueRejectCommand(
        UUID orderId,
        String venueId,
        Instant venueTs,
        String venueExecRef,
        String rawEnvelopeJson
) {}
