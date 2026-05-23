-- Phase A.1 of system-documentation/plans/stock-settlement-production-gap-plan.md
-- (gap plan §5.1: real broker confirm contract). Master plan reference: §12.6
-- in plans/oms-fix-gateway-and-settlement.md (trade matching and broker confirms).
--
-- The existing settlement_file_import_batch + broker_settlement_confirm queue
-- only carries execution ids: the production broker file must also carry the
-- broker's economic record (price, qty, fees, settlement date, correction type,
-- raw row JSON) so trade matching (gap plan §5.2) can detect mismatches before
-- the confirm queue advances settlement state. These tables live alongside the
-- v1 fixture path; the matcher and ingest endpoint land in follow-up slices.

CREATE TABLE broker_confirm_batch (
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
    applied_at          TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'received',
    row_count           INT,
    matched_row_count   INT,
    break_row_count     INT,
    error_summary       TEXT,
    CONSTRAINT broker_confirm_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'matching', 'applied', 'failed'))
);

COMMENT ON TABLE broker_confirm_batch IS
    'Economic broker confirm file ingest batches (master plan §12.6; gap plan §5.1). Idempotent on file bytes (sha256) and on (broker_id, broker_file_id).';

CREATE UNIQUE INDEX uq_broker_confirm_batch_sha
    ON broker_confirm_batch (file_sha256_hex);

CREATE UNIQUE INDEX uq_broker_confirm_batch_broker_file
    ON broker_confirm_batch (broker_id, broker_file_id);

CREATE INDEX idx_broker_confirm_batch_received
    ON broker_confirm_batch (received_at DESC);

CREATE TABLE broker_trade_confirm (
    id                       BIGSERIAL PRIMARY KEY,
    batch_id                 BIGINT      NOT NULL REFERENCES broker_confirm_batch (id) ON DELETE CASCADE,
    broker_id                TEXT        NOT NULL,
    broker_trade_id          TEXT        NOT NULL,
    venue_exec_ref           TEXT,
    account_id               UUID,
    broker_account           TEXT,
    custody_account_id       UUID REFERENCES custody_accounts (id),
    instrument_symbol        TEXT        NOT NULL,
    instrument_isin          TEXT,
    instrument_mic           TEXT,
    instrument_currency      TEXT,
    side                     TEXT        NOT NULL,
    quantity                 NUMERIC(28, 10) NOT NULL,
    price                    NUMERIC(28, 10) NOT NULL,
    gross_amount             NUMERIC(28, 10),
    trade_date               DATE,
    settlement_date          DATE,
    settlement_currency      TEXT,
    broker_status            TEXT,
    correction_type          TEXT        NOT NULL DEFAULT 'new',
    raw_row_json             JSONB       NOT NULL,
    resolved_execution_id    BIGINT      REFERENCES executions (id) ON DELETE SET NULL,
    match_status             TEXT        NOT NULL DEFAULT 'pending',
    match_decided_at         TIMESTAMPTZ,
    match_diff_json          JSONB,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT broker_trade_confirm_side_chk CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT broker_trade_confirm_correction_chk
        CHECK (correction_type IN ('new', 'amend', 'cancel', 'bust')),
    CONSTRAINT broker_trade_confirm_match_status_chk
        CHECK (match_status IN ('pending', 'matched', 'mismatch', 'unresolved', 'waived'))
);

COMMENT ON TABLE broker_trade_confirm IS
    'One row per broker trade confirm (gap plan §5.1). Unique on (broker_id, broker_trade_id); resolved to executions.id by matcher (gap plan §5.2).';

CREATE UNIQUE INDEX uq_broker_trade_confirm_broker_trade
    ON broker_trade_confirm (broker_id, broker_trade_id);

CREATE INDEX idx_broker_trade_confirm_batch
    ON broker_trade_confirm (batch_id);

CREATE INDEX idx_broker_trade_confirm_execution
    ON broker_trade_confirm (resolved_execution_id)
    WHERE resolved_execution_id IS NOT NULL;

CREATE INDEX idx_broker_trade_confirm_match_pending
    ON broker_trade_confirm (created_at)
    WHERE match_status = 'pending';

CREATE TABLE broker_trade_confirm_fee (
    id                  BIGSERIAL PRIMARY KEY,
    confirm_id          BIGINT      NOT NULL REFERENCES broker_trade_confirm (id) ON DELETE CASCADE,
    fee_type            TEXT        NOT NULL,
    amount              NUMERIC(28, 10) NOT NULL,
    currency            TEXT        NOT NULL,
    charged_to          TEXT        NOT NULL DEFAULT 'customer',
    CONSTRAINT broker_trade_confirm_fee_charged_to_chk
        CHECK (charged_to IN ('customer', 'bank', 'tax_authority'))
);

COMMENT ON TABLE broker_trade_confirm_fee IS
    'Per-confirm fee rows (commission, exchange fee, stamp duty, withholding tax, ...). Gap plan §5.12.';

CREATE INDEX idx_broker_trade_confirm_fee_confirm
    ON broker_trade_confirm_fee (confirm_id);
