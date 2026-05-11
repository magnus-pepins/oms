package com.balh.oms.ledger;

import java.math.BigDecimal;

/**
 * Read-only balance row from Ledger {@code GET /balances/{id}} for BFF/FX nostro panels.
 */
public record LedgerBalanceReadModel(
        String balanceId,
        BigDecimal availableBalance,
        BigDecimal bookedBalance,
        String currency,
        String identityId) {}
