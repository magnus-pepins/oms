# Ledger cash / securities posting on settlement transitions (design)

OMS already updates **`positions`** and **`position_history`** when **`executions.settlement_status`** reaches **`settled`** (see [settlement.md](settlement.md) §12.3). **Ledger** remains the system of record for **cash** and for **securities** balances that must reconcile with custody / nostro.

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

When **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true`** with **`OMS_LEDGER_ENABLED=true`**, **`LedgerSettlementOutboxReconciler`** runs on a fixed delay: it locks eligible unposted rows (`SELECT … FOR UPDATE SKIP LOCKED` inside a short transaction), **`POST`**s JSON `{ outboxId, executionId, toSettlementStatus, payload }` to **`OMS_LEDGER_SETTLEMENT_POSTING_HTTP_PATH`** (default **`/internal/v0/settlement-outbox`**) with **`X-Ledger-Key`**, and on **2xx** sets **`posted_at`**. Non-2xx or transport errors increment **`oms_ledger_settlement_outbox_failed_total`** and leave **`posted_at`** null for retry.

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

## References

- [systems/ledger.md](../../system-documentation/systems/ledger.md) (sibling documentation repo; adjust path if your checkout layout differs).
- OMS **`LedgerInflightReservationClient`** + **`ledger_inflight_outbox`** for the existing BUY inflight pattern.
