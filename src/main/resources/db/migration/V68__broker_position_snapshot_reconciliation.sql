-- Phase C Slice 8a (gap plan §5.6): broker position snapshot ingest + daily OMS compare.

CREATE TABLE broker_position_snapshot_batch (
    id                  BIGSERIAL PRIMARY KEY,
    broker_id           TEXT        NOT NULL,
    broker_file_id      TEXT        NOT NULL,
    business_date       DATE        NOT NULL,
    schema_version      INT         NOT NULL,
    file_sha256_hex     CHAR(64)    NOT NULL,
    source              TEXT        NOT NULL,
    file_name           TEXT        NOT NULL,
    generated_at        TIMESTAMPTZ,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parsed_at           TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'received',
    row_count           INT,
    error_summary       TEXT,
    CONSTRAINT broker_position_snapshot_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'failed'))
);

CREATE UNIQUE INDEX uq_broker_position_snapshot_batch_sha
    ON broker_position_snapshot_batch (file_sha256_hex);

CREATE UNIQUE INDEX uq_broker_position_snapshot_batch_broker_file
    ON broker_position_snapshot_batch (broker_id, broker_file_id);

CREATE TABLE broker_position_snapshot_row (
    id                           BIGSERIAL PRIMARY KEY,
    batch_id                     BIGINT      NOT NULL REFERENCES broker_position_snapshot_batch (id) ON DELETE CASCADE,
    broker_id                    TEXT        NOT NULL,
    broker_account               TEXT,
    account_id                   UUID,
    custody_account_id           UUID        REFERENCES custody_accounts (id),
    instrument_symbol            TEXT        NOT NULL,
    instrument_isin              TEXT,
    instrument_currency          TEXT,
    quantity_total               NUMERIC(28, 10) NOT NULL,
    quantity_settled             NUMERIC(28, 10) NOT NULL DEFAULT 0,
    quantity_pending_buy_settle  NUMERIC(28, 10) NOT NULL DEFAULT 0,
    quantity_pending_sell_settle NUMERIC(28, 10) NOT NULL DEFAULT 0,
    as_of                        TIMESTAMPTZ,
    raw_row_json                 JSONB       NOT NULL,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_broker_position_snapshot_row_batch_key
    ON broker_position_snapshot_row (
        batch_id,
        account_id,
        instrument_symbol,
        COALESCE(custody_account_id, '00000000-0000-0000-0000-000000000000'::uuid)
    );

CREATE TABLE position_reconciliation_report (
    id                      BIGSERIAL PRIMARY KEY,
    batch_id                BIGINT      NOT NULL REFERENCES broker_position_snapshot_batch (id) ON DELETE CASCADE,
    broker_id               TEXT        NOT NULL,
    business_date           DATE        NOT NULL,
    status                  TEXT        NOT NULL DEFAULT 'completed',
    row_count               INT         NOT NULL DEFAULT 0,
    matched_count           INT         NOT NULL DEFAULT 0,
    mismatch_count          INT         NOT NULL DEFAULT 0,
    missing_in_oms_count    INT         NOT NULL DEFAULT 0,
    missing_in_broker_count INT         NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT position_reconciliation_report_status_chk
        CHECK (status IN ('completed', 'failed'))
);

CREATE INDEX idx_position_reconciliation_report_batch
    ON position_reconciliation_report (batch_id);

CREATE TABLE position_reconciliation_report_row (
    id                           BIGSERIAL PRIMARY KEY,
    report_id                    BIGINT      NOT NULL REFERENCES position_reconciliation_report (id) ON DELETE CASCADE,
    outcome                      TEXT        NOT NULL,
    account_id                   UUID,
    instrument_symbol            TEXT        NOT NULL,
    custody_account_id           UUID,
    broker_quantity_total        NUMERIC(28, 10),
    oms_quantity_total           NUMERIC(28, 10),
    diff_json                    JSONB       NOT NULL DEFAULT '{}'::JSONB,
    break_id                     BIGINT      REFERENCES reconciliation_breaks (id) ON DELETE SET NULL,
    CONSTRAINT position_reconciliation_report_row_outcome_chk
        CHECK (outcome IN ('matched', 'mismatch', 'missing_in_oms', 'missing_in_broker'))
);

CREATE INDEX idx_position_reconciliation_report_row_report
    ON position_reconciliation_report_row (report_id);
