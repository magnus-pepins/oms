package com.balh.oms.ledger;

import java.math.BigDecimal;

/**
 * Read-only Ledger HTTP access for risk gates (buying power, etc.).
 */
public interface LedgerBalanceClient {

    /**
     * Fetches {@code availableBalance} for a balance, using {@code with_queued=true}
     * so scheduled debits reduce available funds conservatively.
     *
     * @param balanceId Ledger {@code balance_id} (e.g. {@code balance_...})
     */
    BigDecimal fetchAvailableBalance(String balanceId) throws LedgerServiceException;

    /**
     * Ledger balance row for FX nostro read model (same GET as {@link #fetchAvailableBalance}).
     */
    LedgerBalanceReadModel fetchBalanceReadModel(String balanceId) throws LedgerServiceException;

    /**
     * Ledger {@code identity_id} owning the balance (from GET body {@code identityId} or nested
     * {@code identity.identityId}).
     *
     * @throws LedgerServiceException when the balance is missing, the response is invalid, or HTTP fails
     */
    String fetchIdentityIdForBalance(String balanceId) throws LedgerServiceException;

    final class LedgerServiceException extends Exception {
        public LedgerServiceException(String message) {
            super(message);
        }

        public LedgerServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
