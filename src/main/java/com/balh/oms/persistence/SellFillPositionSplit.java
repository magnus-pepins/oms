package com.balh.oms.persistence;

import java.math.BigDecimal;

/**
 * How a SELL {@code TRADE} fill was applied to {@code positions} (for exact unwind on mark-failed).
 */
public record SellFillPositionSplit(BigDecimal fromPendingBuy, BigDecimal fromSettled) {}
