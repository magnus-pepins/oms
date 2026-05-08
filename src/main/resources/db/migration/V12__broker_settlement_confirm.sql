-- Slice 6: broker-side affirmation queue (Shape A). One row per execution once the
-- broker confirms the trade for settlement matching; worker advances settlement_status.

CREATE TABLE broker_settlement_confirm (
    id              BIGSERIAL PRIMARY KEY,
    execution_id    BIGINT NOT NULL UNIQUE REFERENCES executions (id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at      TIMESTAMPTZ
);

COMMENT ON TABLE broker_settlement_confirm IS
    'Broker trade confirmation (§12.6); drives executed→matched→…→settled when processed.';

CREATE INDEX idx_broker_settlement_confirm_pending
    ON broker_settlement_confirm (applied_at)
    WHERE applied_at IS NULL;
