package com.balh.oms.returnpath;

import java.time.Instant;
import java.util.UUID;

/**
 * Venue acceptance of a previously-admitted order: FIX {@code 35=8} with
 * {@code ExecType=New (150=0)} / {@code OrdStatus=New (39=0)}. Drives the cluster's
 * {@code EXEC_TYPE_VENUE_NEW} transition (PENDING_NEW &rarr; WORKING). No fill quantity/price.
 */
public record ExecutionVenueNewCommand(UUID orderId, String venueId, Instant venueTs, String venueExecRef) {}
