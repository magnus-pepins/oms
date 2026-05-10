package com.balh.oms.settlement;

import java.util.List;

/**
 * Result of resolving {@link BrokerFixtureRow} lines to {@code executions.id} (same rules as
 * {@link SettlementConfirmProcessor#registerBrokerConfirmsFromFixture(java.util.List)}).
 */
public record BrokerFixtureResolutionOutcome(
        List<Long> resolvedExecutionIds, int skippedInvalidRows, int skippedUnresolvedRows) {

    public BrokerFixtureResolutionOutcome {
        resolvedExecutionIds =
                resolvedExecutionIds == null ? List.of() : List.copyOf(resolvedExecutionIds);
    }
}
