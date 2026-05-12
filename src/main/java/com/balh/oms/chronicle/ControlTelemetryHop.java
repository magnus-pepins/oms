package com.balh.oms.chronicle;

import java.time.Instant;

/** One pipeline hop materialized on {@link com.balh.oms.proto.control.v1.ControlPendingEvent}. */
public record ControlTelemetryHop(String stage, Instant observedAt) {}
