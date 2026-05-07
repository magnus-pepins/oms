-- OMS slice 1 initial schema.
-- Invariants:
--   1. orders is the system of record. Every authoritative state change must be
--      committed here BEFORE any Chronicle append (see oms/docs/architecture.md).
--   2. control_outbox carries committed work that still needs to be appended to
--      Chronicle. The reconciler picks up rows where chronicle_enqueued_at IS NULL.
--   3. orders.version is the per-row CAS counter and serves as the canonical
--      event_seq for downstream consumers (drop copy, desk UI, NATS fanout).

CREATE TYPE order_status AS ENUM (
    'PENDING_NEW',
    'NEW',
    'WORKING',
    'PARTIALLY_FILLED',
    'FILLED',
    'CANCELLED',
    'REJECTED',
    'EXPIRED'
);

CREATE TYPE order_side AS ENUM ('BUY', 'SELL');

-- Canonical reject taxonomy. Slice 1 ships at least RISK_STALE_QUEUE and
-- RISK_DUPLICATE; the full set in plan §5.11 is filled as milestones land.
CREATE TYPE reject_code AS ENUM (
    'RISK_STALE_QUEUE',
    'RISK_DUPLICATE',
    'RISK_KILL_SWITCH',
    'RISK_BUYING_POWER',
    'RISK_INVALID_INSTRUMENT',
    'RISK_FAT_FINGER_PRICE',
    'RISK_FAT_FINGER_SIZE',
    'RISK_RATE_LIMIT',
    'INTERNAL_ERROR'
);

CREATE TABLE orders (
    id                       UUID PRIMARY KEY,
    account_id               UUID                NOT NULL,
    client_idempotency_key   TEXT                NOT NULL,
    shard_id                 INTEGER             NOT NULL,
    version                  INTEGER             NOT NULL DEFAULT 0,

    status                   order_status        NOT NULL DEFAULT 'PENDING_NEW',
    terminal_reason          reject_code,

    side                     order_side          NOT NULL,
    instrument_symbol        TEXT                NOT NULL,
    quantity                 NUMERIC(28, 10)     NOT NULL,
    limit_price              NUMERIC(28, 10),
    time_in_force            TEXT                NOT NULL,

    received_at              TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    accepted_at              TIMESTAMPTZ,
    terminal_at              TIMESTAMPTZ,

    -- Hashed account_id snapshot, kept on the row to avoid re-hashing in metrics.
    -- Raw account_id stays in the column above; the hash is what shows up in labels.
    account_id_hash          TEXT                NOT NULL,

    UNIQUE (account_id, client_idempotency_key)
);

CREATE INDEX idx_orders_shard_status ON orders (shard_id, status);
CREATE INDEX idx_orders_account ON orders (account_id);

CREATE TABLE control_outbox (
    id                       BIGSERIAL PRIMARY KEY,
    order_id                 UUID                NOT NULL REFERENCES orders(id),
    -- The version of the order this outbox row reflects. Used by the tailer
    -- to apply CAS updates idempotently.
    order_version            INTEGER             NOT NULL,
    -- Free-form payload that the tailer will replay to drive control logic.
    payload                  JSONB               NOT NULL,
    enqueued_at              TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    -- NULL until the row has been successfully appended to Chronicle.
    chronicle_enqueued_at    TIMESTAMPTZ,
    -- Bumped by the reconciler each time a Chronicle append is retried.
    attempts                 INTEGER             NOT NULL DEFAULT 0,
    last_attempt_at          TIMESTAMPTZ,
    last_error               TEXT
);

CREATE INDEX idx_control_outbox_pending
    ON control_outbox (enqueued_at)
    WHERE chronicle_enqueued_at IS NULL;

COMMENT ON TABLE orders IS
    'System of record for OMS orders. orders.version is the per-row CAS counter and serves as event_seq.';
COMMENT ON TABLE control_outbox IS
    'Transactional outbox: rows are committed inside the same Postgres transaction as the orders update, then appended to Chronicle by the OutboxReconciler.';
