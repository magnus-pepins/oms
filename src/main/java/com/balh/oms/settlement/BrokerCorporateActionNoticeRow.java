package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.LocalDate;

/** One corporate-action notice in the gap plan §5.9 broker file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerCorporateActionNoticeRow(
        String brokerEventId,
        String instrumentSymbol,
        String actionType,
        LocalDate effectiveDate,
        LocalDate exDate,
        LocalDate recordDate,
        LocalDate payableDate) {}
