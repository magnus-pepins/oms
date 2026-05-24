-- Phase D Slice 14a/14b + Phase E valuation stub (gap plan §5.8 / §5.9 / §5.10).

ALTER TABLE broker_settlement_fail_row
    ADD COLUMN penalty_booked_at TIMESTAMPTZ;

ALTER TABLE corporate_action_event
    ADD COLUMN record_date DATE,
    ADD COLUMN payable_date DATE,
    ADD COLUMN processing_error TEXT;

CREATE TABLE corporate_action_entitlement (
    id                          BIGSERIAL PRIMARY KEY,
    corporate_action_event_id   BIGINT      NOT NULL REFERENCES corporate_action_event (id) ON DELETE CASCADE,
    account_id                  UUID        NOT NULL,
    instrument_symbol           TEXT        NOT NULL,
    quantity_held               NUMERIC(28, 10) NOT NULL,
    entitlement_quantity        NUMERIC(28, 10),
    entitlement_amount          NUMERIC(28, 10),
    entitlement_currency        TEXT,
    calculated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_corporate_action_entitlement_event_account
        UNIQUE (corporate_action_event_id, account_id)
);

CREATE TABLE corporate_action_position_impact (
    id                          BIGSERIAL PRIMARY KEY,
    corporate_action_event_id   BIGINT      NOT NULL REFERENCES corporate_action_event (id) ON DELETE CASCADE,
    account_id                  UUID        NOT NULL,
    instrument_symbol           TEXT        NOT NULL,
    quantity_before             NUMERIC(28, 10) NOT NULL,
    quantity_after              NUMERIC(28, 10) NOT NULL,
    applied_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_corporate_action_position_impact_event_account
        UNIQUE (corporate_action_event_id, account_id)
);

CREATE TABLE corporate_action_cash_impact (
    id                          BIGSERIAL PRIMARY KEY,
    corporate_action_event_id   BIGINT      NOT NULL REFERENCES corporate_action_event (id) ON DELETE CASCADE,
    account_id                  UUID        NOT NULL,
    gross_amount                NUMERIC(28, 10) NOT NULL,
    net_amount                  NUMERIC(28, 10) NOT NULL,
    currency                    TEXT        NOT NULL,
    payable_date                DATE,
    ledger_outbox_enqueued      BOOLEAN     NOT NULL DEFAULT FALSE,
    applied_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_corporate_action_cash_impact_event_account
        UNIQUE (corporate_action_event_id, account_id)
);

CREATE TABLE isk_valuation_snapshot (
    id                  BIGSERIAL PRIMARY KEY,
    isk_account_id      UUID        NOT NULL,
    account_id          UUID        NOT NULL,
    quarter_start       DATE        NOT NULL,
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cash_sek            NUMERIC(28, 10) NOT NULL DEFAULT 0,
    securities_sek      NUMERIC(28, 10) NOT NULL DEFAULT 0,
    total_sek           NUMERIC(28, 10) NOT NULL,
    valuation_source    TEXT        NOT NULL DEFAULT 'oms_stub',
    CONSTRAINT uq_isk_valuation_snapshot_account_quarter
        UNIQUE (isk_account_id, quarter_start)
);

CREATE TABLE isk_tax_year_export (
    id                      BIGSERIAL PRIMARY KEY,
    tax_year                INT         NOT NULL,
    isk_account_id          UUID        NOT NULL,
    public_account_number   TEXT,
    kapitalunderlag_sek     NUMERIC(28, 10),
    schablonintakt_sek      NUMERIC(28, 10),
    ku30_status             TEXT        NOT NULL DEFAULT 'draft',
    export_json             JSONB       NOT NULL DEFAULT '{}'::JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT isk_tax_year_export_status_chk
        CHECK (ku30_status IN ('draft', 'approved', 'filed')),
    CONSTRAINT uq_isk_tax_year_export_year_account
        UNIQUE (tax_year, isk_account_id)
);
