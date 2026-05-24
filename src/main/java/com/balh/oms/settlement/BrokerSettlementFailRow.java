package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One fail row in the gap plan §5.8 broker settlement fail file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerSettlementFailRow(
        String brokerFailId,
        String brokerTradeId,
        String executionRef,
        String instrumentSymbol,
        String side,
        BigDecimal failedQuantity,
        LocalDate intendedSettlementDate,
        String failReason,
        LocalDate expectedResolutionDate,
        BigDecimal penaltyAmount,
        String penaltyCurrency,
        String resolutionStatus) {}
