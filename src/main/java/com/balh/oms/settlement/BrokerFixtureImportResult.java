package com.balh.oms.settlement;

/** Result of {@link SettlementConfirmProcessor#registerBrokerConfirmsFromFixture(java.util.List, int)}. */
public record BrokerFixtureImportResult(int insertedRows, int skippedUnresolvedRows, int skippedInvalidRows) {}
