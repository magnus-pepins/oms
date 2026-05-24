-- Gap plan Phase A tail: append-only corrections reference the original broker trade id.

ALTER TABLE broker_trade_confirm
    ADD COLUMN IF NOT EXISTS original_broker_trade_id TEXT;

COMMENT ON COLUMN broker_trade_confirm.original_broker_trade_id IS
    'When correction_type is amend/cancel/bust, references the prior broker_trade_id row being corrected.';

CREATE INDEX IF NOT EXISTS idx_broker_trade_confirm_original_trade
    ON broker_trade_confirm (broker_id, original_broker_trade_id)
    WHERE original_broker_trade_id IS NOT NULL;
