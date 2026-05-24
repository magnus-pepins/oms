-- Phase D/E tail: CA payable-date ledger legs, fail customer notifications, withholding.

ALTER TABLE corporate_action_cash_impact
    ADD COLUMN withholding_amount NUMERIC(28, 10),
    ADD COLUMN ledger_outbox_enqueued_at TIMESTAMPTZ;

ALTER TABLE broker_settlement_fail_row
    ADD COLUMN customer_notified_at TIMESTAMPTZ;

CREATE TABLE corporate_action_ledger_outbox (
    id                      BIGSERIAL PRIMARY KEY,
    cash_impact_id          BIGINT        NOT NULL REFERENCES corporate_action_cash_impact (id) ON DELETE CASCADE,
    leg_kind                TEXT          NOT NULL,
    payload_json            JSONB         NOT NULL DEFAULT '{}'::JSONB,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    posted_at               TIMESTAMPTZ,
    skipped_at              TIMESTAMPTZ,
    attempts                INTEGER       NOT NULL DEFAULT 0,
    last_error              TEXT,
    CONSTRAINT uq_corporate_action_ledger_outbox_impact_leg
        UNIQUE (cash_impact_id, leg_kind)
);

CREATE INDEX idx_corporate_action_ledger_outbox_unposted
    ON corporate_action_ledger_outbox (created_at)
    WHERE posted_at IS NULL AND skipped_at IS NULL;

CREATE TABLE settlement_customer_notification_outbox (
    id                  BIGSERIAL PRIMARY KEY,
    notification_type TEXT        NOT NULL,
    account_id          UUID        NOT NULL,
    execution_id        BIGINT,
    fail_row_id         BIGINT,
    envelope_json       JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMPTZ,
    attempts            INTEGER     NOT NULL DEFAULT 0,
    last_error          TEXT
);

CREATE INDEX idx_settlement_customer_notification_pending
    ON settlement_customer_notification_outbox (created_at)
    WHERE published_at IS NULL;
