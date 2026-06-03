-- Per-contract allowed jurisdictions (ISO 3166-1 alpha-2). Empty = no extra constraint beyond BFF env gate.

ALTER TABLE prediction_market_contract
    ADD COLUMN jurisdiction_tags JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN prediction_market_contract.jurisdiction_tags IS
    'JSON array of ISO-3166 alpha-2 country codes allowed to trade this contract; [] = inherit global MARKETS_RETAIL_ALLOWED_COUNTRIES only.';

UPDATE prediction_market_contract
SET jurisdiction_tags = '["SE"]'::jsonb
WHERE slug = 'TEST-1' AND jurisdiction_tags = '[]'::jsonb;
