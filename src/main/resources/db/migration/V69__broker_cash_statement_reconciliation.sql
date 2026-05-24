-- Phase C Slice 8b (gap plan §5.7): broker cash statement ingest + OMS/Ledger movement compare.

CREATE TABLE broker_cash_statement_batch (
    id                  BIGSERIAL PRIMARY KEY,
    broker_id           TEXT        NOT NULL,
    broker_file_id      TEXT        NOT NULL,
    business_date       DATE        NOT NULL,
    currency            TEXT        NOT NULL,
    schema_version      INT         NOT NULL,
    file_sha256_hex     CHAR(64)    NOT NULL,
    source              TEXT        NOT NULL,
    file_name           TEXT        NOT NULL,
    generated_at        TIMESTAMPTZ,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at           TIMESTAMPTZ,
    opening_balance     NUMERIC(28, 10),
    closing_balance     NUMERIC(28, 10),
    status              TEXT        NOT NULL DEFAULT 'received',
    movement_count      INT,
    error_summary       TEXT,
    CONSTRAINT broker_cash_statement_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'failed'))
);

CREATE UNIQUE INDEX uq_broker_cash_statement_batch_sha
    ON broker_cash_statement_batch (file_sha256_hex);

CREATE UNIQUE INDEX uq_broker_cash_statement_batch_broker_file
    ON broker_cash_statement_batch (broker_id, broker_file_id);

CREATE TABLE broker_cash_statement_movement (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            BIGINT      NOT NULL REFERENCES broker_cash_statement_batch (id) ON DELETE CASCADE,
    broker_id           TEXT        NOT NULL,
    broker_movement_id  TEXT        NOT NULL,
    movement_type       TEXT,
    execution_ref       TEXT,
    amount              NUMERIC(28, 10) NOT NULL,
    currency            TEXT        NOT NULL,
    value_date          DATE,
    raw_row_json        JSONB       NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_broker_cash_statement_movement_batch_id
    ON broker_cash_statement_movement (batch_id, broker_movement_id);

CREATE TABLE cash_reconciliation_report (
    id                      BIGSERIAL PRIMARY KEY,
    batch_id                BIGINT      NOT NULL REFERENCES broker_cash_statement_batch (id) ON DELETE CASCADE,
    broker_id               TEXT        NOT NULL,
    business_date           DATE        NOT NULL,
    currency                TEXT        NOT NULL,
    status                  TEXT        NOT NULL DEFAULT 'completed',
    movement_count          INT         NOT NULL DEFAULT 0,
    matched_count           INT         NOT NULL DEFAULT 0,
    mismatch_count          INT         NOT NULL DEFAULT 0,
    unmatched_count         INT         NOT NULL DEFAULT 0,
    missing_in_broker_count INT         NOT NULL DEFAULT 0,
    balance_mismatch        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cash_reconciliation_report_status_chk
        CHECK (status IN ('completed', 'failed'))
);

CREATE INDEX idx_cash_reconciliation_report_batch
    ON cash_reconciliation_report (batch_id);

CREATE TABLE cash_reconciliation_report_row (
    id                  BIGSERIAL PRIMARY KEY,
    report_id           BIGINT      NOT NULL REFERENCES cash_reconciliation_report (id) ON DELETE CASCADE,
    outcome             TEXT        NOT NULL,
    broker_movement_id  TEXT,
    execution_ref       TEXT,
    execution_id        BIGINT,
    account_id          UUID,
    broker_amount       NUMERIC(28, 10),
    oms_amount          NUMERIC(28, 10),
    diff_json           JSONB       NOT NULL DEFAULT '{}'::JSONB,
    break_id            BIGINT      REFERENCES reconciliation_breaks (id) ON DELETE SET NULL,
    CONSTRAINT cash_reconciliation_report_row_outcome_chk
        CHECK (outcome IN ('matched', 'mismatch', 'unmatched', 'missing_in_broker', 'balance_check'))
);

CREATE INDEX idx_cash_reconciliation_report_row_report
    ON cash_reconciliation_report_row (report_id);
