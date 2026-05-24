-- Gap plan Phase B Slice 7b: structured assign / resolve / waive workflow for ops.

ALTER TABLE reconciliation_breaks
    ADD COLUMN IF NOT EXISTS assigned_to TEXT,
    ADD COLUMN IF NOT EXISTS resolution_code TEXT,
    ADD COLUMN IF NOT EXISTS resolution_note TEXT;

COMMENT ON COLUMN reconciliation_breaks.assigned_to IS
    'Operator email when status moves to investigating.';
COMMENT ON COLUMN reconciliation_breaks.resolution_code IS
    'Resolution taxonomy: broker_correct | oms_correct | broker_correction_pending | waived_ops.';
COMMENT ON COLUMN reconciliation_breaks.resolution_note IS
    'Free-text ops note captured on resolve or waive.';

CREATE TABLE IF NOT EXISTS reconciliation_break_events (
    id           BIGSERIAL PRIMARY KEY,
    break_id     BIGINT      NOT NULL REFERENCES reconciliation_breaks (id) ON DELETE CASCADE,
    event_type   TEXT        NOT NULL,
    actor        TEXT        NOT NULL,
    payload_json JSONB       NOT NULL DEFAULT '{}'::JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT reconciliation_break_events_type_chk
        CHECK (event_type IN ('assigned', 'resolved', 'waived', 'note'))
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_break_events_break
    ON reconciliation_break_events (break_id, created_at DESC);

COMMENT ON TABLE reconciliation_break_events IS
    'Append-only audit trail for reconciliation_breaks workflow transitions (gap plan §5.2).';
