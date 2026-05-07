package com.balh.oms.returnpath;

import java.time.Instant;
import java.util.UUID;

public record ExecutionCancelCommand(UUID orderId, String venueId, Instant venueTs, String venueExecRef) {}
