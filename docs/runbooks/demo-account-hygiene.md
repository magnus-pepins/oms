# Demo account hygiene (OMS + Ledger bench)

Operator checklist before demos on **pop** (or any bench with `oms-fix-loopback-acceptor`).

## Magnus demo account (Bug A — canonical USD)

| Field | Value |
|-------|--------|
| `accountId` | `ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970` |
| `ledger_identity_id` | `identity_4a08657c-e7ac-4625-8cc0-6dedec0f01ec` |
| Canonical balance | `balance_b6f5a7eb-5005-4518-94cf-612623809224` |
| Indicator | `inv-ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970-USD` |

**Before demo:**

```bash
# Dry-run legacy USD drain into canonical (customer-frontend repo)
cd ~/customer-frontend
LEDGER_API_URL=http://127.0.0.1:30100 LEDGER_API_KEY=... \
  node scripts/j9-consolidate-dual-usd-balances.mjs \
  --account-id ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970 \
  --identity-id identity_4a08657c-e7ac-4625-8cc0-6dedec0f01ec \
  --dry-run
# Omit --dry-run only when dry-run lists transfers to apply
```

**Verify:** one row for `GET /balances?indicator=inv-ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970-USD`; internal BUY moves only that balance.

## Smoke-test fixture accounts (unfunded outbox noise)

These accounts often have OMS orders but **no** `inv-{accountId}-USD` Ledger balance. The reconciler tombstones after 10 attempts; until then logs show `unfunded_balance`.

| `accountId` | Notes |
|-------------|--------|
| `d2bc6c86-b79b-44df-ad5c-5f18fc43a67b` | E2E smoke fixture |
| `5d88fdc6-859b-4e7e-a22d-b2442108c8e9` | E2E smoke fixture |

**Option A — seed minimal Ledger balance (preferred for demos):**

```bash
cd ~/oms
LEDGER_API_URL=http://127.0.0.1:30100 LEDGER_KEY=... \
  ./scripts/seed-smoke-fixture-invest-balances.sh
```

**Option B — tombstone / delete orphan outbox (no Ledger funding):**

```bash
psql -h 127.0.0.1 -U postgres -d oms -f scripts/sql/tombstone-unfunded-outbox-fixtures.sql
```

**Verify:** `pm2 logs oms-postgres-projector --lines 200` — no rapid repeat of the same `indicator=inv-d2bc6c86…` / `inv-5d88fdc6…` unfunded line (throttled to once per minute after Phase 2C).

## Related

- [oms-cluster-restart.md](oms-cluster-restart.md)
- [../settlement.md](../settlement.md) — loopback SELL reject, poison pills
