-- Phase C: retail contract catalog (YES/NO trade symbols per binary contract).

CREATE TABLE prediction_market_contract (
    id                 BIGSERIAL PRIMARY KEY,
    slug               TEXT          NOT NULL,
    title              TEXT          NOT NULL,
    yes_symbol         TEXT          NOT NULL,
    no_symbol          TEXT          NOT NULL,
    resolution_source  TEXT,
    status             TEXT          NOT NULL DEFAULT 'OPEN',
    tick_size          NUMERIC(10, 4) NOT NULL DEFAULT 0.01,
    payout_per_contract NUMERIC(10, 2) NOT NULL DEFAULT 1.00,
    closes_at          TIMESTAMPTZ,
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_prediction_market_contract_slug UNIQUE (slug),
    CONSTRAINT uq_prediction_market_contract_yes_symbol UNIQUE (yes_symbol),
    CONSTRAINT uq_prediction_market_contract_no_symbol UNIQUE (no_symbol)
);

COMMENT ON TABLE prediction_market_contract IS
    'Phase C retail catalog: maps display contract to YES/NO OMS instrument symbols.';

INSERT INTO prediction_market_contract (
    slug, title, yes_symbol, no_symbol, resolution_source, status, closes_at
) VALUES (
    'TEST-1',
    'Will the integration test event resolve YES?',
    'PREDMKT-TEST-1',
    'PREDMKT-TEST-1-NO',
    'it-oracle',
    'OPEN',
    NOW() + INTERVAL '30 days'
)
ON CONFLICT (slug) DO NOTHING;
