-- Post-commit Ledger sync inflight hold: row written in the SAME transaction as the
-- order insert when async mode is enabled; LedgerInflightOutboxReconciler calls Ledger after commit.
CREATE TABLE ledger_inflight_outbox (
    id               BIGSERIAL PRIMARY KEY,
    order_id         UUID                NOT NULL REFERENCES orders (id),
    payload_json     JSONB               NOT NULL,
    created_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         INTEGER             NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    last_error       TEXT
);

CREATE UNIQUE INDEX uq_ledger_inflight_outbox_order_id
    ON ledger_inflight_outbox (order_id);

CREATE INDEX idx_ledger_inflight_outbox_pending
    ON ledger_inflight_outbox (created_at)
    WHERE published_at IS NULL;

COMMENT ON TABLE ledger_inflight_outbox IS
    'Transactional outbox: BUY inflight hold parameters written with order accept; reconciler POSTs to Ledger after commit.';
