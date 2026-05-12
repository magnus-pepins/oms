-- Idempotent gate for first NOS route enqueue from Chronicle tail when oms.control.postgres-write-path=ingress.
-- Not a second system of record for FIX send intent (see plans/oms-ingress-control-fix-topology.md P1); prevents
-- duplicate offers to the in-process outbound queue on journal replay / duplicate tail delivery.
CREATE TABLE fix_nos_route_enqueue_claim (
    order_id UUID PRIMARY KEY,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE fix_nos_route_enqueue_claim IS
    'One row per order: first successful INSERT wins NOS enqueue from ingress dispatch-only tail; ON CONFLICT skips duplicate enqueue.';
