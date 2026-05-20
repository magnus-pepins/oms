-- One-shot: tombstone unposted ledger_settlement_outbox rows for known smoke fixture accounts
-- when inv-{accountId}-USD was never provisioned in Ledger (Phase 2B option B).
--
-- Prefer seed-smoke-fixture-invest-balances.sh when you want real settlement legs.
-- Run: psql -h 127.0.0.1 -U postgres -d oms -f scripts/sql/tombstone-unfunded-outbox-fixtures.sql

UPDATE ledger_settlement_outbox o
SET skipped_at = NOW(),
    skip_reason = 'operator_tombstone_unfunded_fixture_2026-05-20'
WHERE o.posted_at IS NULL
  AND o.skipped_at IS NULL
  AND o.payload_json::text LIKE ANY (ARRAY[
    '%d2bc6c86-b79b-44df-ad5c-5f18fc43a67b%',
    '%5d88fdc6-859b-4e7e-a22d-b2442108c8e9%'
  ]);

-- Show remaining unposted (non-tombstoned) count
SELECT COUNT(*) AS unposted_remaining
FROM ledger_settlement_outbox
WHERE posted_at IS NULL AND skipped_at IS NULL;
