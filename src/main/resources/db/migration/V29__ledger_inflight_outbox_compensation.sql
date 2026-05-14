-- Phase 4 slice 4p (system-documentation/plans/oms-aeron-cluster-substrate.md):
-- Compensation tracking for the async ledger-inflight-hold path. When a hold fails past the
-- retry threshold, LedgerInflightHoldFailureCompensator submits a CancelOrderCommand to the
-- cluster and stamps `compensated_at` so the row is excluded from further reconciler retries.
--
-- `cancel_correlation_id` records the correlation id used on the cluster CancelOrderCommand;
-- captured for ops debugging only — the cluster log is the durability boundary, not this row.
ALTER TABLE ledger_inflight_outbox
    ADD COLUMN compensated_at         TIMESTAMPTZ,
    ADD COLUMN cancel_correlation_id  BIGINT;

-- Compensator's own working set: rows that have failed (attempts > 0, never published) and have
-- not yet been compensated. The compensator filter additionally clamps on the
-- `attempts >= threshold` config value at query time. Index supports both that scan and the
-- reconciler's "pending and not yet compensated" filter (slice 4p amends the reconciler to
-- include `compensated_at IS NULL` so a compensated row is never re-published to Ledger).
CREATE INDEX idx_ledger_inflight_outbox_failed_uncompensated
    ON ledger_inflight_outbox (attempts, last_attempt_at)
    WHERE published_at IS NULL AND compensated_at IS NULL;

COMMENT ON COLUMN ledger_inflight_outbox.compensated_at IS
    'Set when LedgerInflightHoldFailureCompensator successfully submitted a CancelOrderCommand for this row. Excludes the row from further reconciler retries (see idx_ledger_inflight_outbox_failed_uncompensated).';

COMMENT ON COLUMN ledger_inflight_outbox.cancel_correlation_id IS
    'Correlation id used on the cluster CancelOrderCommand for ops debugging only. Cluster log is the durability boundary; if this column is non-null but the order is not CANCELLED, look at orders.status (a fill can race the compensator).';
