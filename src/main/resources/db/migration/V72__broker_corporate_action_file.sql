-- Phase D Slice 11a (gap plan §5.9): broker corporate-action file ingest skeleton.

CREATE TABLE broker_corporate_action_batch (
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
    event_count         INT,
    error_summary       TEXT,
    CONSTRAINT broker_corporate_action_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'failed', 'applied'))
);

CREATE UNIQUE INDEX uq_broker_corporate_action_batch_sha
    ON broker_corporate_action_batch (file_sha256_hex);

CREATE UNIQUE INDEX uq_broker_corporate_action_batch_broker_file
    ON broker_corporate_action_batch (broker_id, broker_file_id);

CREATE TABLE broker_corporate_action_row (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            BIGINT      NOT NULL REFERENCES broker_corporate_action_batch (id) ON DELETE CASCADE,
    broker_id           TEXT        NOT NULL,
    broker_event_id     TEXT        NOT NULL,
    instrument_symbol   TEXT        NOT NULL,
    action_type         TEXT        NOT NULL,
    effective_date      DATE        NOT NULL,
    raw_row_json        JSONB       NOT NULL,
    corporate_action_event_id BIGINT REFERENCES corporate_action_event (id) ON DELETE SET NULL,
    applied_at          TIMESTAMPTZ,
    apply_error         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_broker_corporate_action_row_batch_id
    ON broker_corporate_action_row (batch_id, broker_event_id);

ALTER TABLE corporate_action_event
    ADD COLUMN broker_id TEXT,
    ADD COLUMN broker_event_id TEXT;

CREATE UNIQUE INDEX uq_corporate_action_event_broker_event
    ON corporate_action_event (broker_id, broker_event_id);
