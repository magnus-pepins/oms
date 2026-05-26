-- Phase B: venue contract resolution + prediction-market Ledger outbox (dispute window before post).

CREATE TABLE venue_contract_resolution (
    id                       BIGSERIAL PRIMARY KEY,
    contract_symbol          TEXT          NOT NULL,
    outcome                  TEXT          NOT NULL,
    resolution_source        TEXT          NOT NULL,
    resolution_timestamp     TIMESTAMPTZ   NOT NULL,
    evidence_hash            TEXT          NOT NULL,
    venue_id                 TEXT          NOT NULL,
    dispute_until            TIMESTAMPTZ   NOT NULL,
    posting_paused           BOOLEAN       NOT NULL DEFAULT FALSE,
    orders_resolved_count    INT           NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_venue_contract_resolution_symbol_hash UNIQUE (contract_symbol, evidence_hash)
);

COMMENT ON TABLE venue_contract_resolution IS
    'Binary contract resolution applied by OMS cluster; dispute window gates Ledger posting.';

CREATE TABLE prediction_market_ledger_outbox (
    id              BIGSERIAL PRIMARY KEY,
    resolution_id   BIGINT        NOT NULL REFERENCES venue_contract_resolution (id) ON DELETE CASCADE,
    account_id      UUID          NOT NULL,
    leg_kind        TEXT          NOT NULL DEFAULT 'prediction-payout',
    payload_json    JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    posted_at       TIMESTAMPTZ,
    skipped_at      TIMESTAMPTZ,
    skip_reason     TEXT,
    attempts        INT           NOT NULL DEFAULT 0,
    last_error_text TEXT,
    last_attempt_at TIMESTAMPTZ,
    CONSTRAINT uq_prediction_market_ledger_outbox_resolution_account_leg
        UNIQUE (resolution_id, account_id, leg_kind)
);

CREATE INDEX idx_prediction_market_ledger_outbox_unposted
    ON prediction_market_ledger_outbox (created_at)
    WHERE posted_at IS NULL AND skipped_at IS NULL;

COMMENT ON TABLE prediction_market_ledger_outbox IS
    'Ledger payout legs for prediction_market_binary_resolution template (Phase B).';
