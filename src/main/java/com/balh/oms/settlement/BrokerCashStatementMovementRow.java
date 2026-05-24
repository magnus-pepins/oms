package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One movement row in the gap plan §5.7 broker cash statement file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerCashStatementMovementRow(
        String brokerMovementId,
        String type,
        String executionRef,
        BigDecimal amount,
        String currency,
        LocalDate valueDate) {}
