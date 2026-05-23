package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

/**
 * One fee row attached to a {@link BrokerTradeConfirmRow}
 * (system-documentation/plans/stock-settlement-production-gap-plan.md §5.12).
 *
 * <p>{@code type} examples: {@code commission}, {@code exchange_fee},
 * {@code clearing_fee}, {@code stamp_duty}, {@code transaction_tax},
 * {@code fx_spread}, {@code withholding_tax}, {@code settlement_penalty}.
 *
 * <p>{@code chargedTo} is {@code customer} (default) for fees passed through to
 * the customer cash leg, {@code bank} for fees absorbed by Balh P&amp;L, or
 * {@code tax_authority} for pass-through taxes (no revenue booking).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerTradeConfirmFeeRow(
        String type, BigDecimal amount, String currency, String chargedTo) {}
