-- V35: extend execution_exec_type with REPLACE / CANCEL_REJECT / REPLACE_REJECT.
--
-- Why: until now the projector only handled venue TRADE / CANCEL / VENUE_REJECT
-- (the only three EXEC_TYPE_* codes the cluster emitted). The Wed-demo modify
-- path completed the cluster side (EXEC_TYPE_REPLACE=3, EXEC_TYPE_CANCEL_REJECT=4,
-- EXEC_TYPE_REPLACE_REJECT=5 — see ApplyExecutionReportCommand) but no migration /
-- repository / projector code was added, so the projector logged "unknown
-- execTypeCode 3" and the audit row never landed. The Wed-morning demo bug
-- ("modify from 1 to 2 stayed at 1") was the user-visible symptom.
--
-- Postgres ALTER TYPE ADD VALUE is non-transactional in <14 and transactional
-- in >=14, but new values cannot be referenced inside the same transaction in
-- which they were added (PG docs). This migration only ADDs values; it does not
-- INSERT using them, so it runs cleanly under Flyway's default per-migration
-- transaction on PG 14+. The values become usable after Flyway commits this
-- migration, before any code path that uses them runs.
--
-- exec_type usage:
--   REPLACE         — venue/broker ACK of a 35=G replace; carries new total qty
--                     (last_quantity column) and new limit price (last_price column).
--   CANCEL_REJECT   — venue/broker decline of a 35=F cancel; no fill, no state
--                     change to the order — purely an audit row + UI toast input.
--   REPLACE_REJECT  — venue/broker decline of a 35=G replace; same semantics as
--                     CANCEL_REJECT, the order is unchanged.

ALTER TYPE execution_exec_type ADD VALUE IF NOT EXISTS 'REPLACE';
ALTER TYPE execution_exec_type ADD VALUE IF NOT EXISTS 'CANCEL_REJECT';
ALTER TYPE execution_exec_type ADD VALUE IF NOT EXISTS 'REPLACE_REJECT';
