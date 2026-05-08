-- Slice 6 spine (Phase 1 §12.2–12.3): positions, custody_accounts, position_history,
-- executions.settlement_status (state machine persisted per execution row).

CREATE TYPE execution_settlement_status AS ENUM (
    'executed',
    'matched',
    'confirmed',
    'settling',
    'settled',
    'failed'
);

ALTER TABLE executions
    ADD COLUMN settlement_status execution_settlement_status NOT NULL DEFAULT 'executed';

COMMENT ON COLUMN executions.settlement_status IS
    'Broker settlement lifecycle (§12.3); new fills start at executed.';

CREATE TABLE custody_accounts (
    id              UUID PRIMARY KEY,
    broker_id       TEXT        NOT NULL,
    account_type    TEXT        NOT NULL,
    csd_or_book_ref TEXT        NOT NULL DEFAULT '',
    currency_class  TEXT        NOT NULL DEFAULT 'MULTI',
    effective_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    terminated_at   TIMESTAMPTZ,
    CONSTRAINT custody_accounts_account_type_chk
        CHECK (account_type IN ('omnibus', 'segregated', 'nominee'))
);

COMMENT ON TABLE custody_accounts IS
    'Broker custody routing (Shape A v1); typically one omnibus per broker (§12.2).';

-- Deterministic default omnibus for v1 single-broker wiring (overridable via app config).
INSERT INTO custody_accounts (id, broker_id, account_type, csd_or_book_ref, currency_class)
VALUES (
    'a0000001-0000-4000-8000-000000000001',
    'DEFAULT',
    'omnibus',
    '',
    'MULTI'
);

CREATE TABLE positions (
    account_id                   UUID NOT NULL,
    instrument_symbol            TEXT NOT NULL,
    custody_account_id           UUID NOT NULL REFERENCES custody_accounts (id),
    quantity_total               NUMERIC(28, 10) NOT NULL DEFAULT 0,
    quantity_settled             NUMERIC(28, 10) NOT NULL DEFAULT 0,
    quantity_pending_buy_settle  NUMERIC(28, 10) NOT NULL DEFAULT 0,
    quantity_pending_sell_settle NUMERIC(28, 10) NOT NULL DEFAULT 0,
    avg_cost_amount              NUMERIC(28, 10),
    currency                     TEXT NOT NULL DEFAULT 'USD',
    last_corporate_action_event_id BIGINT,
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, instrument_symbol, custody_account_id)
);

COMMENT ON TABLE positions IS
    'Customer positions (§12.2); BUY fills increase total + pending buy settle until settlement events.';

CREATE INDEX idx_positions_account ON positions (account_id);

CREATE TABLE position_history (
    id                 BIGSERIAL PRIMARY KEY,
    account_id         UUID NOT NULL,
    instrument_symbol  TEXT NOT NULL,
    custody_account_id UUID NOT NULL REFERENCES custody_accounts (id),
    event_type         TEXT NOT NULL,
    quantity_delta     NUMERIC(28, 10) NOT NULL,
    execution_id       BIGINT REFERENCES executions (id),
    payload_json       JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE position_history IS
    'Append-only position deltas for audit / reconstruct (§12.2).';

CREATE INDEX idx_position_history_account_time
    ON position_history (account_id, created_at);

CREATE TABLE manual_settlement_actions (
    id             BIGSERIAL PRIMARY KEY,
    execution_id   BIGINT NOT NULL REFERENCES executions (id),
    action_type    TEXT NOT NULL,
    requested_by   TEXT NOT NULL,
    approved_by    TEXT,
    payload_json   JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE manual_settlement_actions IS
    'Four-eyes manual settlement instructions (§12.8); Beard Admin integration later.';
