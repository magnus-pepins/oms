ALTER TABLE prediction_market_contract
    ADD COLUMN category TEXT,
    ADD COLUMN tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN card_image_url TEXT,
    ADD COLUMN display_order INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN prediction_market_contract.category IS 'Optional retail catalog grouping label';
COMMENT ON COLUMN prediction_market_contract.tags IS 'Display tags for catalog filtering (JSON array of strings)';
COMMENT ON COLUMN prediction_market_contract.card_image_url IS 'Optional HTTPS thumbnail for catalog cards';
COMMENT ON COLUMN prediction_market_contract.display_order IS 'Lower sorts first in OPEN catalog list';

UPDATE prediction_market_contract SET display_order = id WHERE display_order = 0;
