-- Transactional outbox for domain fanout (desk / drop copy / NATS).
-- Rows are written in the SAME Postgres transaction as the originating order
-- mutation (ingress accept or control CAS). A reconciler delivers after commit.
CREATE TABLE domain_event_outbox (
    id               BIGSERIAL PRIMARY KEY,
    order_id         UUID                NOT NULL REFERENCES orders (id),
    envelope_json    JSONB               NOT NULL,
    created_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         INTEGER             NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    last_error       TEXT
);

CREATE INDEX idx_domain_event_outbox_pending
    ON domain_event_outbox (created_at)
    WHERE published_at IS NULL;

COMMENT ON TABLE domain_event_outbox IS
    'Transactional fanout outbox: envelope written with orders/control changes; DomainFanoutReconciler publishes after commit.';
