-- Slice 6 follow-up: durable idempotency surface for broker EOD / confirms **file** ingest (not HTTP fixture).
-- Worker implementation is tracked in docs/settlement-eod-ingest.md.

CREATE TABLE settlement_file_import_batch (
    id                  BIGSERIAL PRIMARY KEY,
    source              TEXT        NOT NULL,
    file_name           TEXT        NOT NULL,
    file_sha256_hex     CHAR(64)    NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status              TEXT        NOT NULL DEFAULT 'received',
    row_count           INT,
    error_summary       TEXT
);

COMMENT ON TABLE settlement_file_import_batch IS
    'Broker file ingest batches; unique SHA-256 prevents re-processing identical file bytes (see docs/settlement-eod-ingest.md).';

CREATE UNIQUE INDEX uq_settlement_file_import_batch_sha
    ON settlement_file_import_batch (file_sha256_hex);

CREATE INDEX idx_settlement_file_import_batch_received
    ON settlement_file_import_batch (received_at DESC);
