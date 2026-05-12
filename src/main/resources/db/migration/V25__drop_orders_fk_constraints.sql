-- Phase 2 slice 2c of system-documentation/plans/oms-aeron-cluster-substrate.md: Postgres `orders`
-- becomes a downstream projection of the cluster log; `OrderIngressService` no longer writes to it
-- (the projector writes orders rows from `OrderAdmittedEvent` events; see V24__aeron_projector_cursor
-- and OmsPostgresProjector). Tables that hold an order_id can no longer carry a FOREIGN KEY to
-- orders(id), because the ingress transaction would commit BEFORE the projector applies the orders
-- row, deferring or failing every FK check.
--
-- Dropping the FK does NOT weaken the data model: the cluster log is now the source of truth for
-- "this orderId exists and was admitted"; Postgres orders is one of several projections of that
-- truth. Idempotency on (account_id, client_idempotency_key) is enforced inside the cluster
-- service (idempotency index across snapshots) and by the projector's
-- INSERT ... ON CONFLICT DO NOTHING on the same unique constraint that V1 already created.
--
-- Postgres auto-generates FK names as `<table>_<column>_fkey`; we use IF EXISTS so re-running the
-- migration on a database that pre-dates the auto-name (or on a fresh dev DB that names them
-- differently) is a no-op.

ALTER TABLE control_outbox          DROP CONSTRAINT IF EXISTS control_outbox_order_id_fkey;
ALTER TABLE domain_event_outbox     DROP CONSTRAINT IF EXISTS domain_event_outbox_order_id_fkey;
ALTER TABLE ledger_inflight_outbox  DROP CONSTRAINT IF EXISTS ledger_inflight_outbox_order_id_fkey;
ALTER TABLE control_decisions       DROP CONSTRAINT IF EXISTS control_decisions_order_id_fkey;
ALTER TABLE executions              DROP CONSTRAINT IF EXISTS executions_order_id_fkey;
ALTER TABLE market_context          DROP CONSTRAINT IF EXISTS market_context_order_id_fkey;
