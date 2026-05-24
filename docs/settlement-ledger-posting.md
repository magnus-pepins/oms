# Ledger cash / securities posting on settlement transitions (design)

OMS already updates **`positions`** and **`position_history`** when **`executions.settlement_status`** reaches **`settled`** (see [settlement.md](settlement.md) §12.3). **Ledger** remains the system of record for **cash** and for **securities** balances that must reconcile with custody / nostro.

> **Status (2026-05-19 — V39 multi-leg outbox LANDED in OMS):** the single-row, single-Ledger-`POST /settlement-outbox` shape described in §"Proposed outbox shape (Flyway V16)" / §"Payload contract (versioned)" below is **superseded** by the multi-leg model in §"Multi-leg outbox v2 (V39, default in OMS HEAD)" at the bottom of this file. The V16 prose is kept for history because the older `LedgerSettlementOutbox*IntegrationTest` fixtures still reference the v1 schema vocabulary; new code paths read `leg_kind` and call Ledger `/transactions` per leg.

## Principles

1. **Same transactional boundary** as OMS settlement state change: any Ledger API call that must align with a given **`settled`** (or future **failed** cash leg) transition should be **outboxed** in the same Postgres transaction as `executions.updateSettlementStatusIf` / `PositionsRepository.record*Settled`, then processed by an existing reconciler pattern (see **`ledger_inflight_outbox`**).
2. **Idempotency:** Ledger posts keyed by **`execution_id`** + **`transition`** (or Chronicle event id) so retries do not double-post.
3. **Product contract first:** which transitions emit which Ledger journal lines (BUY cash debit, SELL cash credit, CSD fees, FX) is **finance-owned** before implementation.

## Proposed outbox shape (Flyway V16)

Table **`ledger_settlement_outbox`** (see migration `V16__ledger_settlement_outbox.sql`):

| Column | Purpose |
|--------|---------|
| `id` | BIGSERIAL |
| `execution_id` | FK to `executions.id` (`ON DELETE CASCADE`) |
| `to_settlement_status` | Target status (e.g. `settled`) |
| `payload_json` | Ledger adapter body (minimal stub today) |
| `created_at` | Enqueue time |
| `posted_at` | Null until Ledger ACK |

Unique constraint on **`(execution_id, to_settlement_status)`** for idempotency.

OMS enqueues a row when **`OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED=true`** (default **`false`**) on the same transaction as the **`settled`** CAS in **`SettlementConfirmProcessor.applyTransition`**. Payload includes **`schemaVersion`: 1** and **`event`**: `SETTLEMENT_SETTLED` plus execution id, side, symbol, quantity (see payload table below).

When **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true`** with **`OMS_LEDGER_ENABLED=true`**, **`LedgerSettlementOutboxReconciler`** runs on a fixed delay: it locks eligible unposted rows (`SELECT … FOR UPDATE SKIP LOCKED` inside a short transaction), **`POST`**s JSON `{ outboxId, executionId, toSettlementStatus, payload }` to **`OMS_LEDGER_SETTLEMENT_POSTING_HTTP_PATH`** (default **`/internal/v0/settlement-outbox`**) with **`Authorization: Bearer ${OMS_LEDGER_API_KEY}`** (switched from `X-Ledger-Key` on the 2026-05-17 `ledger-cluster-rest-shim` cutover), and on **2xx** sets **`posted_at`**. Non-2xx or transport errors increment **`oms_ledger_settlement_outbox_failed_total`** and leave **`posted_at`** null for retry.

## Payload contract (versioned)

`payload_json` is Ledger-owned semantically; OMS stamps a **`schemaVersion`** so adapters can evolve without silent misreads.

| `schemaVersion` | Shape | Notes |
|-------------------|--------|--------|
| `1` | `{ "schemaVersion": 1, "event": "SETTLEMENT_SETTLED", "executionId": long, "side": "BUY"\|"SELL", "instrumentSymbol": string, "quantity": string }` | Minimal stub shipped with enqueue; finance extends fields in lockstep with Ledger **`/internal/v0/settlement-outbox`** handler. |
| _future_ | TBD | Add new integer versions only; never reuse numbers. Ledger must reject unknown **`schemaVersion`** with **4xx** so OMS leaves **`posted_at`** null and ops can intervene. |

**Idempotency (HTTP):** request body includes **`outboxId`** and **`executionId`**; Ledger must treat **`(executionId, toSettlementStatus)`** as idempotent (same as OMS unique constraint).

**Error handling:** **5xx** or transport failure → retry; **4xx** (except **409** idempotent duplicate if Ledger chooses) → dead-letter policy is ops-owned (manual clear or tooling); OMS does not delete outbox rows automatically on 4xx today.

## Wiring points in OMS

- **`SettlementConfirmProcessor.applyTransition`** when `next == "settled"` — after `positions.recordBuySettled` / `recordSellSettled` and successful **`executions.updateSettlementStatusIf`**, **`LedgerSettlementOutboxRepository.insertIgnore`** runs when the flag is on.
- **`markTradeFailed`** — optional cash/hold reversal rows (if inflight holds exist for the order).

## Multi-leg outbox v2 (V39, default in OMS HEAD)

The Phase 1 demo cuts the v1 single-row / `POST /settlement-outbox` design above for two reasons: Ledger never grew a dedicated settlement-outbox endpoint (the request body would just open and apply a transaction anyway), and a settled trade has two financially distinct movements — **cash** and **commission** — that an operator wants to retry independently when one side errors. The multi-leg shape models each movement as its own outbox row and lets `LedgerSettlementLegPoster` translate one row to one `POST /transactions`. Same idempotency + reconciler skeleton as v1.

### Migration **V39__ledger_settlement_outbox_leg_kind.sql**

Adds `leg_kind TEXT NOT NULL DEFAULT 'cash'`; replaces unique index `uq_ledger_settlement_outbox_execution_status` with `uq_ledger_settlement_outbox_exec_status_leg(execution_id, to_settlement_status, leg_kind)` so each leg has its own posted_at lifecycle. Existing pre-V39 rows are backfilled to `'cash'` by the column default.

`LedgerSettlementOutboxRepository` exposes four leg constants:

| Constant | When emitted | Ledger transaction shape |
|----------|--------------|--------------------------|
| `LEG_CASH` | Single-currency BUY or SELL (`cashCurrency == tradeCurrency`). Phase 1 default. | `inv-<accountId>-<ccy>` ↔ `@Nostro-<tradeCcy>-Bank` for `notional` in `tradeCurrency`. BUY debits customer / credits nostro; SELL reverses. |
| `LEG_CASH_BASE` | Phase 2 cross-currency cash leg, customer side. | `inv-<accountId>-<cashCcy>` ↔ `@FX-Suspense-<cashCcy>` for `cashAmount` in `cashCurrency`. Single-currency `rate=1` per Ledger model; mirrors `FxHedgeService`. |
| `LEG_CASH_QUOTE` | Phase 2 cross-currency cash leg, bank side. | `@FX-Suspense-<tradeCcy>` ↔ `@Nostro-<tradeCcy>-Bank` for `notional` in `tradeCurrency`. Pairs with `LEG_CASH_BASE`; both clear when the FX hedge resolves the suspense. |
| `LEG_FEE` | Whenever `feeAmount > 0` (mirrors customer-frontend `resolveStockFee`). | `inv-<accountId>-<feeCcy>` → `feeBalanceIndicator` (default `@Fees-<feeCcy>`) for `feeAmount` in `feeCurrency`. |

### Enriched payload contract (`schemaVersion=2`)

Enqueued by `SettlementConfirmProcessor.enqueueSettlementLegs(...)` when `OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED=true` and the execution carries a non-null `last_price`. Required fields per row:

```jsonc
{
  "schemaVersion": 2,
  "event": "SETTLEMENT_SETTLED",
  "executionId": 12345,
  "accountId": "5e8a…",
  "side": "BUY",                  // or "SELL"
  "instrumentSymbol": "AAPL",
  "market": "US",                 // resolves the default fee schedule
  "quantity": "1",                // string to preserve precision
  "price": "298.45",
  "tradeCurrency": "USD",         // execution currency (where price lives)
  "cashCurrency": "USD",          // customer cash account currency
  "notional": "298.45",           // quantity × price, BigDecimal-formatted
  "feeAmount": "1.00",            // 0 → fee leg is skipped at post time
  "feeCurrency": "USD",           // typically == cashCurrency
  "feeBalanceIndicator": "@Fees-USD",  // optional override; default @Fees-<feeCcy>
  "settledAt": "2026-05-19T13:21:01.234Z"
}
```

Defaults for `market` and `cashCurrency` come from `oms.settlement.default-instrument-market` (default `US`) and `oms.settlement.default-cash-currency` (default `USD`); override per deploy via `OMS_SETTLEMENT_DEFAULT_INSTRUMENT_MARKET` / `OMS_SETTLEMENT_DEFAULT_CASH_CURRENCY` once executions stop matching the defaults.

### Idempotency

Each `POST /transactions` uses a deterministic `reference`:

- `LEG_CASH` → `settlement-<outboxId>-cash`
- `LEG_CASH_BASE` → `settlement-<outboxId>-cash-base`
- `LEG_CASH_QUOTE` → `settlement-<outboxId>-cash-quote`
- `LEG_FEE` → `settlement-<outboxId>-fee`

Ledger enforces `reference` uniqueness on the Postgres API path (`@unique` in Prisma; duplicate POST returns `409 CONFLICT`). Settlement leg posting uses a deterministic reference (`settlement-<outboxId>-<leg>`) and idempotency via `GET /transactions?reference=…` before submit plus treating `409` as success when the reference already exists (`LedgerSettlementLegPoster`). The outbox `uq_…_leg_kind` index plus the `posted_at IS NULL` reconciler filter keep duplicates from being submitted in the happy path.

### Bank-side balances the demo needs seeded

| Indicator | Used by | Seeded by |
|-----------|---------|-----------|
| `@Nostro-USD-Bank` / `@Nostro-EUR-Bank` / `@Nostro-GBP-Bank` | `LEG_CASH` (single-ccy) and `LEG_CASH_QUOTE` (cross-ccy). | `oms/scripts/seed-fx-nostros.sh` (already on Pop from the FX slice). |
| `@FX-Suspense-USD` / `@FX-Suspense-EUR` / `@FX-Suspense-GBP` | `LEG_CASH_BASE` / `LEG_CASH_QUOTE`. | Same script as nostros. |
| `@Fees-USD` / `@Fees-EUR` / `@Fees-GBP` | `LEG_FEE`. | `oms/scripts/seed-ledger-settlement.sh` (Phase 1 commission revenue). |

Customer-side `inv-<accountId>-<ccy>` balances are created on first sign-in by the customer-frontend; nothing new to seed there for settlement.

### Wiring in OMS

- `SettlementConfirmProcessor.applyTransitionAndCleanupTrade(…)` → `enqueueSettlementLegs(snapshot)` — runs inside the same DB transaction as the `executions.settlement_status='settled'` CAS, so an outbox enqueue failure rolls the status back to retry on the next confirm.
- `LedgerSettlementOutboxReconciler` — unchanged loop; now passes `leg_kind` to `LedgerSettlementPostingClient.postSettlementOutbox(…)`.
- `LedgerSettlementLegPoster` (in `com.balh.oms.ledger`) — dispatches on `legKind`, builds the Ledger body, posts to `/transactions`, logs `outboxId leg=… txn=<ledger txn id>` on success.
- `StockCommissionCalculator` — mirrors the customer-frontend `STOCK_<market>` default schedule (US: 0.25 % notional, min $1, max $50, half-up to cents; EU / UK have their own constants in the same class). Used by `SettlementConfirmProcessor` to compute `feeAmount` when the payload is built.

### Operator levers

Both flags live in `application.yaml` under `oms.ledger`:

- `OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED` (`oms.ledger.settlement-outbox-enabled`, default `false`) — gate on enqueue side.
- `OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED` (`oms.ledger.settlement-outbox-reconciler-enabled`, default `false`) — gate on poster side. Both must be `true` for an end-to-end flow; PM2 ecosystem flips them for the demo.

The `OMS_LEDGER_SETTLEMENT_POSTING_HTTP_PATH` knob from v1 is now unused (`LedgerSettlementLegPoster` always targets `/transactions`); the property is kept in `application.yaml` for backwards compat but the poster ignores it.

## References

- [systems/ledger.md](../../system-documentation/systems/ledger.md) (sibling documentation repo; adjust path if your checkout layout differs).
- OMS **`LedgerInflightReservationClient`** + **`ledger_inflight_outbox`** for the existing BUY inflight pattern.
- `oms/src/main/java/com/balh/oms/ledger/LedgerSettlementLegPoster.java` — V39 multi-leg poster.
- `oms/src/main/java/com/balh/oms/settlement/StockCommissionCalculator.java` — Phase 1 fee schedule (mirrors customer-frontend).
- `oms/scripts/seed-ledger-settlement.sh` — creates the `@Fees-<ccy>` revenue balances.
