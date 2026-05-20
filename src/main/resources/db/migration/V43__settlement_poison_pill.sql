-- OMS-2: auto-step scheduler failure cap (stop infinite retry on stuck settlement advances).
-- OMS-3: ledger outbox skip tombstone (stop reconciler polling unpostable demo rows).

ALTER TABLE executions
    ADD COLUMN settlement_auto_step_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN settlement_auto_step_last_failure_at TIMESTAMPTZ,
    ADD COLUMN settlement_auto_step_last_error TEXT;

COMMENT ON COLUMN executions.settlement_auto_step_failures IS
    'Consecutive SettlementAutoStepScheduler advance failures; reset on success.';

ALTER TABLE ledger_settlement_outbox
    ADD COLUMN skipped_at TIMESTAMPTZ,
    ADD COLUMN skip_reason TEXT;

COMMENT ON COLUMN ledger_settlement_outbox.skipped_at IS
    'When set, reconciler stops delivery attempts (operator/data gap, not a Ledger post).';

CREATE INDEX idx_ledger_settlement_outbox_active_unposted
    ON ledger_settlement_outbox (id)
    WHERE posted_at IS NULL AND skipped_at IS NULL;
