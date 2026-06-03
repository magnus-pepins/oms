-- Phase C: retail-facing contract copy (detail page + partner deep-links).

ALTER TABLE prediction_market_contract
    ADD COLUMN description          TEXT,
    ADD COLUMN resolution_criteria  TEXT,
    ADD COLUMN reference_links      JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN resolves_at          TIMESTAMPTZ;

COMMENT ON COLUMN prediction_market_contract.description IS
    'Longer narrative shown on the customer contract detail page.';
COMMENT ON COLUMN prediction_market_contract.resolution_criteria IS
    'Plain-English rules for how YES/NO is determined at resolution.';
COMMENT ON COLUMN prediction_market_contract.reference_links IS
    'JSON array of {label, url} objects (https external sources).';
COMMENT ON COLUMN prediction_market_contract.resolves_at IS
    'Expected resolution timestamp (display / ops; oracle may differ).';
