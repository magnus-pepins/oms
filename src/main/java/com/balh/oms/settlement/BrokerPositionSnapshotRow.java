package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** One row in the gap plan §5.6 broker position snapshot file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerPositionSnapshotRow(
        String brokerAccount,
        UUID accountId,
        UUID custodyAccountId,
        Instrument instrument,
        BigDecimal quantityTotal,
        BigDecimal quantitySettled,
        BigDecimal quantityPendingBuySettle,
        BigDecimal quantityPendingSellSettle,
        Instant asOf) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Instrument(String symbol, String isin, String currency) {}
}
