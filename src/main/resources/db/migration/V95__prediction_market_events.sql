-- Phase H: catalog events group multiple binary contracts on one retail card (Polymarket-style).

CREATE TABLE prediction_market_event (
    id              BIGSERIAL PRIMARY KEY,
    slug            TEXT          NOT NULL,
    title           TEXT          NOT NULL,
    description     TEXT,
    category        TEXT,
    tags            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    card_image_url  TEXT,
    display_order   INTEGER       NOT NULL DEFAULT 0,
    status          TEXT          NOT NULL DEFAULT 'DRAFT',
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_prediction_market_event_slug UNIQUE (slug)
);

COMMENT ON TABLE prediction_market_event IS
    'Retail catalog grouping: one event card may list several binary contracts as outcome rows.';

ALTER TABLE prediction_market_contract
    ADD COLUMN event_id BIGINT REFERENCES prediction_market_event (id) ON DELETE SET NULL,
    ADD COLUMN outcome_label TEXT,
    ADD COLUMN outcome_display_order INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_prediction_market_contract_event_id
    ON prediction_market_contract (event_id)
    WHERE event_id IS NOT NULL;

COMMENT ON COLUMN prediction_market_contract.event_id IS
    'Optional parent event for grouped catalog cards; NULL = standalone card.';
COMMENT ON COLUMN prediction_market_contract.outcome_label IS
    'Short row label inside an event card (e.g. Mexico, Trump).';
COMMENT ON COLUMN prediction_market_contract.outcome_display_order IS
    'Sort order of outcome rows within the parent event card.';
