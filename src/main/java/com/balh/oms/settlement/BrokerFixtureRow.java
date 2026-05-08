package com.balh.oms.settlement;

import java.util.UUID;

/**
 * One row from a broker confirms JSON fixture: either {@code executionId} or
 * {@code (accountId, venueExecRef)} to resolve a {@code TRADE} execution.
 */
public record BrokerFixtureRow(Long executionId, UUID accountId, String venueExecRef) {}
