package com.balh.oms.ledger;

import java.math.BigDecimal;
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

    class LedgerIskReadException extends Exception {
        public LedgerIskReadException(String message) {
            super(message);
        }

        public LedgerIskReadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    List<DepositRow> listDeposits(String iskAccountId, Instant from, Instant to) throws LedgerIskReadException;
}
