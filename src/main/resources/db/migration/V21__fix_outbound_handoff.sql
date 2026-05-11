-- Durable handoff queue: control path enqueues WORKING order ids; single FIX outbound consumer pops with SKIP LOCKED.
CREATE TABLE fix_outbound_handoff (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fix_outbound_handoff_created ON fix_outbound_handoff (created_at);
