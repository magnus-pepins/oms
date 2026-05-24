package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * One broker trade confirm row inside {@link BrokerTradeConfirmEnvelope}
 * (system-documentation/plans/stock-settlement-production-gap-plan.md §5.1).
 *
 * <p>Quantity and price are strings on the wire (carried here as {@link BigDecimal}
 * via Jackson's default conversion) so brokers can preserve precision; do not
 * truncate at the API edge.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerTradeConfirmRow(
        String brokerTradeId,
        String venueExecRef,
        UUID accountId,
        String brokerAccount,
        UUID custodyAccountId,
        Instrument instrument,
        String side,
        BigDecimal quantity,
        BigDecimal price,
        BigDecimal grossAmount,
        List<BrokerTradeConfirmFeeRow> fees,
        LocalDate tradeDate,
        LocalDate settlementDate,
        String settlementCurrency,
        String status,
        String correctionType,
        String originalBrokerTradeId) {

    /**
     * Instrument identification block. Symbol is required; ISIN / MIC / currency are
     * optional on the wire because not every broker supplies all three on every row.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Instrument(String symbol, String isin, String mic, String currency) {}
}
