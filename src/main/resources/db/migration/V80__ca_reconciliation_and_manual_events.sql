-- Phase 1 v1 tail: CA broker reconciliation + cash recon nostro columns (gap plan §5.9 / §5.7).

ALTER TABLE cash_reconciliation_report
    ADD COLUMN IF NOT EXISTS nostro_balance_mismatch BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE cash_reconciliation_report_row
    DROP CONSTRAINT IF EXISTS cash_reconciliation_report_row_outcome_chk;

ALTER TABLE cash_reconciliation_report_row
    ADD CONSTRAINT cash_reconciliation_report_row_outcome_chk
        CHECK (outcome IN (
            'matched', 'mismatch', 'unmatched', 'missing_in_broker', 'balance_check',
            'nostro_balance_mismatch', 'ledger_cash_leg_mismatch'
        ));

CREATE TABLE corporate_action_reconciliation_report (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT      NOT NULL REFERENCES broker_corporate_action_batch (id) ON DELETE CASCADE,
    broker_id       TEXT        NOT NULL,
    business_date   DATE        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'completed',
    event_count     INT         NOT NULL DEFAULT 0,
    matched_count   INT         NOT NULL DEFAULT 0,
    mismatch_count  INT         NOT NULL DEFAULT 0,
    unmatched_count INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT corporate_action_reconciliation_report_status_chk
        CHECK (status IN ('completed', 'failed'))
);

CREATE INDEX idx_corporate_action_reconciliation_report_batch
    ON corporate_action_reconciliation_report (batch_id);

CREATE TABLE corporate_action_reconciliation_report_row (
    id                      BIGSERIAL PRIMARY KEY,
    report_id               BIGINT      NOT NULL REFERENCES corporate_action_reconciliation_report (id) ON DELETE CASCADE,
    outcome                 TEXT        NOT NULL,
    broker_event_id         TEXT,
    corporate_action_event_id BIGINT,
    action_type             TEXT,
    instrument_symbol       TEXT,
    diff_json               JSONB       NOT NULL DEFAULT '{}'::JSONB,
    break_id                BIGINT      REFERENCES reconciliation_breaks (id) ON DELETE SET NULL,
    CONSTRAINT corporate_action_reconciliation_report_row_outcome_chk
        CHECK (outcome IN ('matched', 'mismatch', 'unmatched'))
);

CREATE INDEX idx_corporate_action_reconciliation_report_row_report
    ON corporate_action_reconciliation_report_row (report_id);

-- Manual CA events (merger / spin-off / bankruptcy templates) with four-eyes approval.
CREATE TABLE manual_corporate_action_event (
    id                  BIGSERIAL PRIMARY KEY,
    template_type       TEXT        NOT NULL,
    payload_json        JSONB       NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'pending_approval',
    created_by          TEXT        NOT NULL,
    approved_by         TEXT,
    approved_at         TIMESTAMPTZ,
    corporate_action_event_id BIGINT REFERENCES corporate_action_event (id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT manual_corporate_action_event_status_chk
        CHECK (status IN ('pending_approval', 'approved', 'rejected', 'applied')),
    CONSTRAINT manual_corporate_action_event_template_chk
        CHECK (template_type IN ('MERGER', 'SPIN_OFF', 'BANKRUPTCY_DELISTING'))
);

CREATE INDEX idx_manual_corporate_action_event_status
    ON manual_corporate_action_event (status, created_at DESC);
