package com.balh.oms.ledger;

import java.time.Instant;
import java.util.List;

/** Elevated read client for ledger ISK projector facts (LC-ISK-8). */
public interface LedgerIskReadClient {

    record DepositRow(
            long amountMinor,
            String currency,
            String depositClass,
            boolean countsTowardKapitalunderlag,
            Instant effectiveAt) {}

    /**
     * Subset of ledger {@code isk_accounts} relevant to OMS-side KU30 export.
     * The source of truth is the ledger projector ({@code isk_accounts.public_account_number} is
     * populated by {@code IskEnsureBalanceService} / {@code IskAccountsProjectorSupport}).
     * KU30 ruta 817 = {@link #publicAccountNumber()} — never confuse with internal
     * {@code ledger_balance_id}.
     */
    record IskAccountRow(String iskAccountId, String publicAccountNumber) {}

    class LedgerIskReadException extends Exception {
        public LedgerIskReadException(String message) {
            super(message);
        }

        public LedgerIskReadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    List<DepositRow> listDeposits(String iskAccountId, Instant from, Instant to) throws LedgerIskReadException;

    /**
     * List all ISK accounts visible to OMS (no identity filter). Used by the KU30 export to
     * resolve each iskAccountId → public_account_number for ruta 817. The ledger endpoint
     * currently caps at 200 rows; the export job iterates {@code oms_account_tax_wrapper}
     * (the OMS-side mapping) and looks up the public number in the returned map.
     */
    List<IskAccountRow> listAccounts() throws LedgerIskReadException;
}
