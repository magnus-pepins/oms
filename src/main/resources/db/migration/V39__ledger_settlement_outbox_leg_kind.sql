-- Extend ledger_settlement_outbox to support multi-leg posting (cash + fee, and
-- in Phase 2 cash-base + cash-quote for cross-currency settlements). Each leg
-- is enqueued as its own row keyed by (execution_id, to_settlement_status,
-- leg_kind) so retries and posted_at are tracked per leg — a fee leg failing
-- does not undo a successfully-posted cash leg.
--
-- See plans/oms-fix-gateway-and-settlement.md §11 (settlement) for the design.

ALTER TABLE ledger_settlement_outbox
    ADD COLUMN leg_kind TEXT NOT NULL DEFAULT 'cash';

COMMENT ON COLUMN ledger_settlement_outbox.leg_kind IS
    'Independently-postable Ledger leg for one settlement transition. One of: '
    || 'cash (single-currency customer cash → @Nostro), '
    || 'cash-base / cash-quote (cross-currency cash via @FX-Suspense, Phase 2), '
    || 'fee (commission → @Fees-<ccy>). '
    || 'Combined with (execution_id, to_settlement_status) for idempotent enqueue.';

-- Replace the (execution_id, to_settlement_status) uniqueness with one that includes leg_kind.
DROP INDEX IF EXISTS uq_ledger_settlement_outbox_execution_status;

CREATE UNIQUE INDEX uq_ledger_settlement_outbox_exec_status_leg
    ON ledger_settlement_outbox (execution_id, to_settlement_status, leg_kind);
