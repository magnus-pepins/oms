package com.balh.oms.persistence;

import java.time.Instant;

/** Row from {@code oms_runtime_flags} for {@code global_halt}. */
public record GlobalHaltRow(boolean value, Instant updatedAt) {}
