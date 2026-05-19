package com.balh.oms.ledger;

import java.math.BigDecimal;

/**
 * Read-only balance row from Ledger {@code GET /balances/{id}} for BFF/FX nostro panels.
 *
 * <p>{@code indicator} is the human-meaningful label Ledger stores per balance
 * (e.g. {@code @Nostro-USD-Bank}, {@code @FX-Suspense-EUR}). Empty string when
 * the Ledger row has no indicator set; never null so the BFF can ship it
 * straight to JSON without null-guards.
 */
public record LedgerBalanceReadModel(
        String balanceId,
        BigDecimal availableBalance,
        BigDecimal bookedBalance,
        String currency,
        String identityId,
        String indicator) {}
