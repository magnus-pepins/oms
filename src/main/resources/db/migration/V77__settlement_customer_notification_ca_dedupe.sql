-- Slice 14c/14d tail: CA dividend customer notifications + outbox dedupe.

ALTER TABLE settlement_customer_notification_outbox
    ADD COLUMN cash_impact_id BIGINT REFERENCES corporate_action_cash_impact (id) ON DELETE SET NULL;

ALTER TABLE corporate_action_cash_impact
    ADD COLUMN customer_notified_at TIMESTAMPTZ;

CREATE UNIQUE INDEX uq_settlement_customer_notification_ca
    ON settlement_customer_notification_outbox (notification_type, cash_impact_id)
    WHERE cash_impact_id IS NOT NULL;

CREATE INDEX idx_settlement_customer_notification_stuck
    ON settlement_customer_notification_outbox (created_at)
    WHERE published_at IS NULL AND attempts >= 3;
