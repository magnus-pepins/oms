-- Phase E: contract-level fee models, execution fee accrual, participant overrides.

ALTER TABLE prediction_market_contract
    ADD COLUMN fee_model_id TEXT NOT NULL DEFAULT 'ZERO',
    ADD COLUMN fee_schedule_version INT NOT NULL DEFAULT 1,
    ADD COLUMN fee_params_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN retail_fee_model_id TEXT NULL;

COMMENT ON COLUMN prediction_market_contract.fee_model_id IS
    'Charging model for this contract (ZERO, TAKER_ONLY, SYMMETRIC, MAKER_TAKER, ALL_IN, KALSHI).';
COMMENT ON COLUMN prediction_market_contract.fee_schedule_version IS
    'Bumped when fee_params change; pinned on orders at accept.';
COMMENT ON COLUMN prediction_market_contract.fee_params_json IS
    'Model-specific rates (bps, Kalshi k, caps).';
COMMENT ON COLUMN prediction_market_contract.retail_fee_model_id IS
    'When set, retail participants use this model instead of fee_model_id on the same contract.';

CREATE TABLE venue_participant_fee_override (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    participant_type TEXT NOT NULL,
    participant_id UUID NOT NULL,
    contract_id BIGINT NULL REFERENCES prediction_market_contract (id) ON DELETE CASCADE,
    fee_model_id TEXT NULL,
    fee_params_json JSONB NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT venue_participant_fee_override_unique
        UNIQUE (participant_type, participant_id, contract_id)
);

COMMENT ON TABLE venue_participant_fee_override IS
    'Commercial fee overrides for FIX counterparties or OMS accounts (Phase E).';

CREATE INDEX idx_venue_participant_fee_override_lookup
    ON venue_participant_fee_override (participant_type, participant_id)
    WHERE enabled = TRUE;

ALTER TABLE executions
    ADD COLUMN liquidity_role TEXT NULL,
    ADD COLUMN fee_amount NUMERIC(20, 8) NULL,
    ADD COLUMN fee_currency TEXT NULL,
    ADD COLUMN fee_model_id TEXT NULL,
    ADD COLUMN fee_schedule_version INT NULL,
    ADD COLUMN fee_collected_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN executions.liquidity_role IS 'MAKER or TAKER for venue-routed TRADE rows (Phase E).';
COMMENT ON COLUMN executions.fee_amount IS 'Accrued trade fee; collected at resolution when fee_collected_at is set.';

ALTER TABLE orders
    ADD COLUMN pinned_fee_model_id TEXT NULL,
    ADD COLUMN pinned_fee_schedule_version INT NULL,
    ADD COLUMN pinned_estimated_fee NUMERIC(20, 8) NULL,
    ADD COLUMN pinned_fee_currency TEXT NULL;

COMMENT ON COLUMN orders.pinned_fee_model_id IS
    'Fee model + version quoted at accept (Phase E PM contracts).';

INSERT INTO settlement_template (template_id, version, outbox_table, description, active)
VALUES (
    'prediction_market_trade_fee',
    1,
    'prediction_market_ledger_outbox',
    'PM contract resolution: aggregate per-fill trade fees debited from customer collateral',
    TRUE
)
ON CONFLICT (template_id, version) DO NOTHING;
