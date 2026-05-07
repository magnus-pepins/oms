-- Slice 3: return path — executions ledger + best-ex stub (market_context).

ALTER TABLE orders
    ADD COLUMN cum_filled_quantity NUMERIC(28, 10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN orders.cum_filled_quantity IS
    'Cumulative filled quantity from applied venue execution reports; Slice 3.';

CREATE TABLE market_context (
    order_id      UUID PRIMARY KEY REFERENCES orders (id) ON DELETE CASCADE,
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshot_json JSONB      NOT NULL DEFAULT '{}'::JSONB
);

COMMENT ON TABLE market_context IS
    'NBBO / reference snapshot at decide time (stub JSON in slice 3; populated on first fill apply).';

CREATE TYPE execution_exec_type AS ENUM ('TRADE', 'CANCEL');

CREATE TABLE executions (
    id                  BIGSERIAL PRIMARY KEY,
    order_id            UUID                NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    account_id          UUID                NOT NULL,
    venue_id            TEXT                NOT NULL,
    venue_ts            TIMESTAMPTZ         NOT NULL,
    venue_exec_ref      TEXT                NOT NULL,
    last_quantity       NUMERIC(28, 10)     NOT NULL,
    last_price          NUMERIC(28, 10),
    leaves_quantity     NUMERIC(28, 10),
    cum_quantity_after  NUMERIC(28, 10)     NOT NULL,
    exec_type           execution_exec_type NOT NULL DEFAULT 'TRADE',
    raw_envelope_json   JSONB               NOT NULL DEFAULT '{}'::JSONB,
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_executions_account_venue_exec
    ON executions (account_id, venue_exec_ref);

CREATE INDEX idx_executions_order_time
    ON executions (order_id, created_at);

COMMENT ON TABLE executions IS
    'Venue execution reports applied to orders; idempotent on (account_id, venue_exec_ref).';
