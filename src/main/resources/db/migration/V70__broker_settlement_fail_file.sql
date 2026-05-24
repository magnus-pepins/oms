-- Phase D Slice 10a (gap plan §5.8): broker settlement fail file ingest skeleton.

CREATE TABLE broker_settlement_fail_batch (
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
    fail_count          INT,
    error_summary       TEXT,
    CONSTRAINT broker_settlement_fail_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'failed'))
);

CREATE UNIQUE INDEX uq_broker_settlement_fail_batch_sha
    ON broker_settlement_fail_batch (file_sha256_hex);

CREATE UNIQUE INDEX uq_broker_settlement_fail_batch_broker_file
    ON broker_settlement_fail_batch (broker_id, broker_file_id);

CREATE TABLE broker_settlement_fail_row (
    id                          BIGSERIAL PRIMARY KEY,
    batch_id                    BIGINT      NOT NULL REFERENCES broker_settlement_fail_batch (id) ON DELETE CASCADE,
    broker_id                   TEXT        NOT NULL,
    broker_fail_id              TEXT        NOT NULL,
    broker_trade_id             TEXT,
    execution_ref               TEXT,
    instrument_symbol           TEXT,
    side                        TEXT,
    failed_quantity             NUMERIC(28, 10) NOT NULL,
    intended_settlement_date    DATE        NOT NULL,
    fail_reason                 TEXT,
    expected_resolution_date    DATE,
    penalty_amount              NUMERIC(28, 10),
    penalty_currency            TEXT,
    resolution_status           TEXT,
    raw_row_json                JSONB       NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_broker_settlement_fail_row_batch_id
    ON broker_settlement_fail_row (batch_id, broker_fail_id);

CREATE INDEX idx_broker_settlement_fail_row_execution_ref
    ON broker_settlement_fail_row (execution_ref);
