-- Per-row forensic state for ledger_settlement_outbox delivery attempts.
-- The reconciler increments attempts and records last_error_text on every tick
-- (success clears via posted_at; failures leave the row unposted for retry).

ALTER TABLE ledger_settlement_outbox
    ADD COLUMN attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN last_error_text TEXT,
    ADD COLUMN last_attempt_at TIMESTAMPTZ;

COMMENT ON COLUMN ledger_settlement_outbox.attempts IS
    'Delivery attempts by LedgerSettlementOutboxReconciler (incremented before each post).';

COMMENT ON COLUMN ledger_settlement_outbox.last_error_text IS
    'Truncated error from the most recent delivery attempt; NULL before first attempt.';

COMMENT ON COLUMN ledger_settlement_outbox.last_attempt_at IS
    'Timestamp of the most recent delivery attempt; NULL before first attempt.';

CREATE INDEX idx_ledger_settlement_outbox_stuck
    ON ledger_settlement_outbox (last_attempt_at DESC)
    WHERE posted_at IS NULL AND attempts > 0;
