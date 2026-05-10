-- Ledger settlement legs: outbox rows enqueued in the same Postgres transaction as OMS settlement CAS
-- when oms.ledger.settlement-outbox-enabled=true (default false). Reconciler → Ledger HTTP is future work.

CREATE TABLE ledger_settlement_outbox (
    id                      BIGSERIAL PRIMARY KEY,
        execution_id          BIGINT        NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    to_settlement_status  TEXT          NOT NULL,
    payload_json          JSONB         NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    posted_at             TIMESTAMPTZ
);

COMMENT ON TABLE ledger_settlement_outbox IS
    'Postings to Ledger for settlement transitions; unique per execution + terminal status (see docs/settlement-ledger-posting.md).';

CREATE UNIQUE INDEX uq_ledger_settlement_outbox_execution_status
    ON ledger_settlement_outbox (execution_id, to_settlement_status);

CREATE INDEX idx_ledger_settlement_outbox_unposted
    ON ledger_settlement_outbox (created_at)
    WHERE posted_at IS NULL;
