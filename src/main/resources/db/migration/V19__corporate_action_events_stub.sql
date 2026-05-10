-- Slice 8 / 12.10.1: minimal corporate-actions inbox (ingest + processing deferred to workers).
CREATE TABLE IF NOT EXISTS corporate_action_event (
    id                 BIGSERIAL PRIMARY KEY,
    instrument_symbol  TEXT        NOT NULL,
    action_type        TEXT        NOT NULL,
    effective_date     DATE        NOT NULL,
    payload_json       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_corporate_action_event_unprocessed
    ON corporate_action_event (effective_date)
    WHERE processed_at IS NULL;

COMMENT ON TABLE corporate_action_event IS 'Corporate action notifications (cash dividend, split, etc.); processors land in later slices.';
