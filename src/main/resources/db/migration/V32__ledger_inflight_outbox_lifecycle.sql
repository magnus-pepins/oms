-- Wed-demo (system-documentation/plans/oms-aeron-cluster-substrate.md): inflight-hold lifecycle
-- reconciler. When an order becomes terminal (FILLED / CANCELLED / REJECTED / EXPIRED), the
-- Ledger inflight hold placed at admission time must be released — committed for a fill,
-- voided for a cancel/reject. The TS Ledger and the ledger-cluster shim both expose
-- PUT /transactions/inflight/{txID} for that purpose, addressed by the Ledger-generated
-- transactionId (txn_<uuid>).
--
-- The OMS placed the hold via POST /transactions, whose response carries this transactionId.
-- Before this migration, RestLedgerInflightReservationClient discarded it; this slice persists
-- it back into ledger_inflight_outbox so the new LedgerInflightLifecycleReconciler can read it
-- and call the lifecycle endpoint with the right addressing.

ALTER TABLE ledger_inflight_outbox
    ADD COLUMN ledger_txn_id             TEXT,                            -- "txn_<uuid>" as returned by POST /transactions; populated on first successful publish.
    ADD COLUMN lifecycle_settled_at      TIMESTAMPTZ,                     -- Set when the inflight lifecycle action (commit/void) succeeded against the ledger shim.
    ADD COLUMN lifecycle_settled_action  TEXT,                            -- 'commit' (order FILLED) or 'void' (order CANCELLED/REJECTED/EXPIRED).
    ADD COLUMN lifecycle_attempts        INT NOT NULL DEFAULT 0,          -- Retry counter for the lifecycle call; bounded by config so failures don't loop forever.
    ADD COLUMN lifecycle_last_error      TEXT,                            -- Last error string from the lifecycle call (truncated by repo layer).
    ADD COLUMN lifecycle_last_attempt_at TIMESTAMPTZ;                     -- Wall-clock of the last lifecycle attempt; used for backoff filtering at the repo.

-- Working-set index for the lifecycle reconciler. A row is settle-eligible iff:
--   * the inflight hold has been published to Ledger (published_at IS NOT NULL → ledger_txn_id populated),
--   * it has NOT already been settled (lifecycle_settled_at IS NULL),
--   * it has NOT been compensated (compensated_at IS NULL — a compensated row's order was cancelled
--     in the cluster before the hold ever landed in Ledger, so there's nothing to settle).
-- The join to `orders` for the terminal-status filter happens at query time; the partial index
-- keeps the working set small even when the orders table is large.
CREATE INDEX idx_ledger_inflight_outbox_lifecycle_pending
    ON ledger_inflight_outbox (id)
    WHERE published_at IS NOT NULL
      AND lifecycle_settled_at IS NULL
      AND compensated_at IS NULL;

COMMENT ON COLUMN ledger_inflight_outbox.ledger_txn_id IS
    'Ledger transactionId ("txn_<uuid>") returned by POST /transactions when the inflight hold was placed. Address used by LedgerInflightLifecycleReconciler on PUT /transactions/inflight/{txID} for commit/void.';

COMMENT ON COLUMN ledger_inflight_outbox.lifecycle_settled_at IS
    'Set when the Ledger inflight lifecycle call (commit on fill, void on cancel/reject) returned 2xx. Excludes the row from further reconciler retries.';

COMMENT ON COLUMN ledger_inflight_outbox.lifecycle_settled_action IS
    'Which terminal lifecycle action settled this hold: ''commit'' (FILLED) or ''void'' (CANCELLED/REJECTED/EXPIRED). Captured for audit and metrics.';

COMMENT ON COLUMN ledger_inflight_outbox.lifecycle_attempts IS
    'Number of lifecycle-call attempts for this row. Bounded by oms.ledger.inflight-lifecycle-attempts-threshold so a permanently-failing call does not loop.';

COMMENT ON COLUMN ledger_inflight_outbox.lifecycle_last_error IS
    'Last error from the lifecycle call (truncated). Empty/null after a successful settle.';

COMMENT ON COLUMN ledger_inflight_outbox.lifecycle_last_attempt_at IS
    'Wall-clock of the most recent lifecycle attempt. Reconciler uses it to enforce a per-row min-backoff between retries.';
