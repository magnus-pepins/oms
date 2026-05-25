-- Phase 2 CA tail: treaty withholding residency, cost-basis on position impacts.
-- FX at scale: nostro correspondent registry + customer-flow netting bucket.

CREATE TABLE oms_account_tax_residency (
    account_id      UUID        PRIMARY KEY,
    tax_country     CHAR(2)     NOT NULL CHECK (tax_country ~ '^[A-Z]{2}$'),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE oms_account_tax_residency IS
    'Customer tax residency (ISO 3166-1 alpha-2) for treaty withholding on corporate-action cash impacts (gap plan §5.9 Phase 2).';

ALTER TABLE corporate_action_position_impact
    ADD COLUMN cost_basis_before NUMERIC(28, 10),
    ADD COLUMN cost_basis_after  NUMERIC(28, 10),
    ADD COLUMN cost_basis_method TEXT;

ALTER TABLE corporate_action_position_impact
    DROP CONSTRAINT uq_corporate_action_position_impact_event_account;

ALTER TABLE corporate_action_position_impact
    ADD CONSTRAINT uq_corporate_action_position_impact_event_account_symbol
        UNIQUE (corporate_action_event_id, account_id, instrument_symbol);

COMMENT ON COLUMN corporate_action_position_impact.cost_basis_method IS
    'Allocation method used (PROPORTIONAL_SHARES, PROPORTIONAL_MARKET_VALUE, ALL_TO_SURVIVOR, SPIN_OFF_FRACTION).';

CREATE TABLE fx_nostro_correspondent (
    id                  BIGSERIAL   PRIMARY KEY,
    currency            CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    correspondent_code  TEXT        NOT NULL CHECK (length(btrim(correspondent_code)) > 0),
    ledger_balance_id   TEXT        NOT NULL CHECK (length(btrim(ledger_balance_id)) > 0),
    priority            INT         NOT NULL DEFAULT 1 CHECK (priority >= 1),
    status              TEXT        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'drained')),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fx_nostro_correspondent_currency_code
        UNIQUE (currency, correspondent_code)
);

CREATE INDEX idx_fx_nostro_correspondent_currency_priority
    ON fx_nostro_correspondent (currency, priority ASC, id ASC);

COMMENT ON TABLE fx_nostro_correspondent IS
    'Per-currency nostro correspondent registry with active failover (master plan §11.5.3).';

CREATE TABLE fx_customer_flow_netting_bucket (
    id              BIGSERIAL       PRIMARY KEY,
    pair            CHAR(6)         NOT NULL CHECK (pair ~ '^[A-Z]{6}$'),
    base_currency   CHAR(3)         NOT NULL,
    quote_currency  CHAR(3)         NOT NULL,
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    net_base_amount NUMERIC(38, 8)  NOT NULL DEFAULT 0,
    net_quote_amount NUMERIC(38, 8) NOT NULL DEFAULT 0,
    flow_count      INT             NOT NULL DEFAULT 0,
    status          TEXT            NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'closed', 'hedged')),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fx_netting_bucket_pair_window UNIQUE (pair, window_start)
);

CREATE INDEX idx_fx_netting_bucket_status_window
    ON fx_customer_flow_netting_bucket (status, window_end DESC);

COMMENT ON TABLE fx_customer_flow_netting_bucket IS
    'Time-windowed customer FX flow aggregation before nostro hedge (§11.5.5).';
