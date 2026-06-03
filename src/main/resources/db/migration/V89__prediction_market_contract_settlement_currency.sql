-- Per-contract settlement currency for prediction-market cash collateral and resolution payouts.

ALTER TABLE prediction_market_contract
    ADD COLUMN settlement_currency CHAR(3) NOT NULL DEFAULT 'USD';

COMMENT ON COLUMN prediction_market_contract.settlement_currency IS
    'ISO 4217 cash currency for order funding and resolution payouts (e.g. USD, EUR, SEK).';

UPDATE prediction_market_contract
SET settlement_currency = 'USD'
WHERE settlement_currency IS NULL OR TRIM(settlement_currency) = '';
