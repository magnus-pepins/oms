package com.balh.oms.ledger;

/**
 * Wed-demo (V32): {@code PUT /transactions/inflight/{txID}} client. Two operations against the
 * Ledger shim: {@code commit} (a hold becomes a posted transaction on order fill) and
 * {@code void} (a hold is released untouched on cancel/reject/expire).
 *
 * <p>The {@code txID} addressing is captured at hold-placement time and persisted in
 * {@code ledger_inflight_outbox.ledger_txn_id}; this client never has to look it up by reference.
 *
 * <p>Implementations should be idempotent at the call level — a second {@code commit} on an
 * already-committed hold returns 2xx (TS Ledger behaviour: looks up the inflight transaction
 * and is a no-op when its state already matches the requested action). Cluster shim mirrors
 * this in {@code InflightController}.
 */
public interface LedgerInflightLifecycleClient {

    /**
     * Settles the inflight hold permanently — debit becomes a posted transaction. Called when
     * the order's status becomes {@code FILLED}.
     */
    void commitHold(String ledgerTxnId) throws LedgerLifecycleException;

    /**
     * Releases the inflight hold untouched — source balance regains the held notional. Called
     * when the order's status becomes {@code CANCELLED}, {@code REJECTED}, or {@code EXPIRED}.
     */
    void voidHold(String ledgerTxnId) throws LedgerLifecycleException;

    final class LedgerLifecycleException extends Exception {
        public LedgerLifecycleException(String message) {
            super(message);
        }

        public LedgerLifecycleException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
