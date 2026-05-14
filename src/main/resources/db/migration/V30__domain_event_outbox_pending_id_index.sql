-- Phase 4 Tier 2.5 phase C-1
-- (system-documentation/plans/oms-aeron-cluster-substrate.md): partial index that lets
-- the planner satisfy DomainFanoutReconciler's
--   SELECT ... FROM domain_event_outbox
--    WHERE published_at IS NULL AND created_at <= $1
--    ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED
-- using an index walk in (id) order **only over unpublished rows**.
--
-- Why we need it: Pop! 2026-05-14 profile-led pivot
-- (oms/docs/runbooks/local-multi-jvm-bench.md ## Profile-led pivot section) showed the
-- planner picking `domain_event_outbox_pkey` for that query because `ORDER BY id` matched
-- pkey ordering. With ~100 unpublished rows out of 3.14 M total in the table, the pkey
-- scan walked 3 138 866 heap rows under a Filter + LockRows to surface ~50 rows. Mean
-- exec time was 747 ms / call (97.6 % of all Postgres time during a 30 k burst), and at
-- peak ~37 % of ingress http-nio threads were stalled inside HikariPool.getConnection
-- waiting for a connection that the reconciler was holding for those 747 ms.
--
-- The existing `idx_domain_event_outbox_pending btree (created_at) WHERE published_at IS
-- NULL` (V3) stays in place — it's still useful for any query that orders by
-- `created_at`. The planner just couldn't use it for the reconciler's `ORDER BY id`
-- shape. This new index closes that specific case.
--
-- CONCURRENTLY: cannot run inside a transaction; the executeInTransaction=false directive
-- below tells Flyway 10 to run this migration with autocommit instead of wrapping in BEGIN.
-- This means the migration is **not atomic** — if it fails partway, an INVALID index can
-- be left behind and must be DROPped before retry. Acceptable trade-off here because the
-- alternative (plain CREATE INDEX) takes an AccessExclusiveLock on a hot table for the
-- duration of the build, which on a 3 M-row Pop!/dev table is a few seconds and on a
-- production-shape table could be much longer.
-- flyway:executeInTransaction=false

-- Note: COMMENT ON INDEX kept in the migration file's own header above (Flyway 10 with
-- executeInTransaction=false does not allow mixing CREATE INDEX CONCURRENTLY with a
-- transactional COMMENT statement in the same migration). Migration file is the
-- canonical source of doc; pg_description carries no extra value here.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_domain_event_outbox_pending_id
    ON domain_event_outbox (id)
    WHERE published_at IS NULL;
