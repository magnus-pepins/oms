-- Phase A.3 of system-documentation/plans/stock-settlement-production-gap-plan.md
-- (gap plan §5.2: trade matching and break management; §5.13 ops UI).
--
-- Single break-queue table for all reconciliation domains. v1 populated by:
--   - BrokerTradeConfirmMatcher (broker confirm vs execution mismatch / unresolved)
-- v2+ populated by:
--   - Position reconciler (broker SOD/EOD positions vs OMS positions)
--   - Cash reconciler (broker cash statement vs Ledger nostro balances)
--   - Corporate action reconciler (broker CA event vs Balh CA event)
--
-- Beard Admin / ops-console reads from this table via the GET endpoint added
-- alongside the matcher. Resolution and notes are out of scope for v1 (operator
-- closes via direct write today; structured workflow lands in §5.13 follow-up).

CREATE TABLE reconciliation_breaks (
    id              BIGSERIAL PRIMARY KEY,
    break_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL DEFAULT 'high',
    source_system   TEXT        NOT NULL DEFAULT 'broker',
    confirm_id      BIGINT      REFERENCES broker_trade_confirm (id) ON DELETE SET NULL,
    execution_id    BIGINT      REFERENCES executions (id) ON DELETE SET NULL,
    account_id      UUID,
    business_date   DATE,
    diff_json       JSONB       NOT NULL DEFAULT '{}'::JSONB,
    status          TEXT        NOT NULL DEFAULT 'open',
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    opened_by       TEXT        NOT NULL DEFAULT 'system',
    resolved_at     TIMESTAMPTZ,
    resolved_by     TEXT,
    notes           TEXT,
    CONSTRAINT reconciliation_breaks_severity_chk
        CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    CONSTRAINT reconciliation_breaks_status_chk
        CHECK (status IN ('open', 'investigating', 'resolved', 'waived')),
    CONSTRAINT reconciliation_breaks_break_type_chk
        CHECK (break_type IN (
            'trade_mismatch',
            'unresolved_confirm',
            'unmatched_execution',
            'cash_mismatch',
            'position_mismatch',
            'corporate_action_mismatch'
        )),
    CONSTRAINT reconciliation_breaks_source_system_chk
        CHECK (source_system IN ('broker', 'csd', 'ledger', 'internal'))
);

COMMENT ON TABLE reconciliation_breaks IS
    'Cross-domain break queue (gap plan §5.2/§5.6/§5.7/§5.9/§5.13). v1 populated by BrokerTradeConfirmMatcher; future reconcilers append into the same table for one beard-admin queue.';

CREATE INDEX idx_reconciliation_breaks_open
    ON reconciliation_breaks (opened_at DESC)
    WHERE status IN ('open', 'investigating');

CREATE INDEX idx_reconciliation_breaks_confirm
    ON reconciliation_breaks (confirm_id)
    WHERE confirm_id IS NOT NULL;

CREATE INDEX idx_reconciliation_breaks_execution
    ON reconciliation_breaks (execution_id)
    WHERE execution_id IS NOT NULL;

CREATE INDEX idx_reconciliation_breaks_account
    ON reconciliation_breaks (account_id, opened_at DESC)
    WHERE account_id IS NOT NULL;
