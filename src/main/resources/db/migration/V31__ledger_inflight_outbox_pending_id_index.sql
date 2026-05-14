-- Phase 4 Tier 2.5 phase C-1 follow-on
-- (system-documentation/plans/oms-aeron-cluster-substrate.md): same shape of fix as V30,
-- but for ledger_inflight_outbox.
--
-- Why we need it: post-V30 verification on Pop! 2026-05-14 showed the bottleneck shifted
-- from `domain_event_outbox` to `ledger_inflight_outbox`. The reconciler SELECT
--   SELECT id, order_id, payload_json::text, created_at, attempts
--   FROM ledger_inflight_outbox
--   WHERE published_at IS NULL AND compensated_at IS NULL AND created_at <= $1
--   ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED
-- still picks `ledger_inflight_outbox_pkey` (because `ORDER BY id` matches pkey
-- ordering), then filters out 42 972 rows under LockRows to surface 200 — 15 ms per call,
-- 10.95 ms mean across the burst (15.2 % of all Postgres time post-V30, top of
-- pg_stat_statements).
--
-- The existing partial index `idx_ledger_inflight_outbox_pending btree (created_at) WHERE
-- published_at IS NULL` (V4) is on the wrong column to satisfy ORDER BY id, and missing
-- the `compensated_at IS NULL` predicate. The existing
-- `idx_ledger_inflight_outbox_failed_uncompensated btree (attempts, last_attempt_at)
-- WHERE published_at IS NULL AND compensated_at IS NULL` (V29) is on the right predicate
-- but ordered by (attempts, last_attempt_at), so the planner cannot use it for ORDER BY
-- id either.
--
-- This new index gives the planner a single index that satisfies all of:
--   (1) WHERE published_at IS NULL  AND compensated_at IS NULL  -- partial predicate
--   (2) ORDER BY id                                             -- index column
-- so the scan walks only matching rows in id order. The other two indexes stay in place;
-- they serve different predicates (the V29 compensator scan uses (attempts,
-- last_attempt_at) ordering, V4 is unused after V30+V31 but kept for safety).
-- flyway:executeInTransaction=false

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_inflight_outbox_pending_id
    ON ledger_inflight_outbox (id)
    WHERE published_at IS NULL AND compensated_at IS NULL;

COMMENT ON INDEX idx_ledger_inflight_outbox_pending_id IS
    'Partial index on (id) WHERE published_at IS NULL AND compensated_at IS NULL. Lets the planner satisfy LedgerInflightOutboxReconciler''s ORDER BY id LIMIT N FOR UPDATE SKIP LOCKED with an index walk over only pending+uncompensated rows, avoiding the pkey scan + Filter that walked 42 972 rows on Pop! 2026-05-14 to surface the first 200. See ## Profile-led pivot in docs/runbooks/local-multi-jvm-bench.md plus the post-V30 verification that motivated this index.';
