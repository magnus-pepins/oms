-- Phase D Slice 10b (gap plan §5.8): partial/full fail lots + broker fail apply path.

CREATE TABLE execution_settlement_lot (
    id                          BIGSERIAL PRIMARY KEY,
    execution_id                BIGINT      NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    quantity                    NUMERIC(28, 10) NOT NULL,
    intended_settlement_date    DATE        NOT NULL,
    actual_settlement_date      DATE,
    status                      TEXT        NOT NULL,
    broker_fail_reason          TEXT,
    broker_ref                  TEXT,
    broker_settlement_fail_row_id BIGINT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT execution_settlement_lot_status_chk
        CHECK (status IN (
            'pending', 'settled', 'failed', 'partially_failed', 'bought_in', 'unwound'
        ))
);

CREATE INDEX idx_execution_settlement_lot_execution
    ON execution_settlement_lot (execution_id);

ALTER TABLE broker_settlement_fail_row
    ADD COLUMN execution_id BIGINT REFERENCES executions (id) ON DELETE SET NULL,
    ADD COLUMN lot_id BIGINT REFERENCES execution_settlement_lot (id) ON DELETE SET NULL,
    ADD COLUMN applied_at TIMESTAMPTZ,
    ADD COLUMN apply_error TEXT;

ALTER TABLE broker_settlement_fail_batch
    DROP CONSTRAINT broker_settlement_fail_batch_status_chk;

ALTER TABLE broker_settlement_fail_batch
    ADD CONSTRAINT broker_settlement_fail_batch_status_chk
        CHECK (status IN ('received', 'parsing', 'parsed', 'failed', 'applied'));

ALTER TABLE reconciliation_breaks
    DROP CONSTRAINT reconciliation_breaks_break_type_chk;

ALTER TABLE reconciliation_breaks
    ADD CONSTRAINT reconciliation_breaks_break_type_chk
        CHECK (break_type IN (
            'trade_mismatch',
            'unresolved_confirm',
            'unmatched_execution',
            'cash_mismatch',
            'position_mismatch',
            'corporate_action_mismatch',
            'settlement_fail_unmatched',
            'settlement_fail_quantity_mismatch'
        ));
