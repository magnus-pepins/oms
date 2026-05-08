# Settlement spine (slice 6 — Phase 1 §12.2–12.3)

This document describes the **securities post-trade** tables and wiring that exist today in OMS. It is **not** the full settlement state machine or Ledger cash legs; those follow the master plan.

## Schema (Flyway `V11__settlement_positions.sql`)

- **`custody_accounts`** — broker custody routing (Shape A v1). A deterministic **DEFAULT** omnibus row is inserted for single-broker wiring.
- **`positions`** — one row per `(account_id, instrument_symbol, custody_account_id)` with quantity columns (`quantity_total`, `quantity_settled`, `quantity_pending_buy_settle`, `quantity_pending_sell_settle`).
- **`position_history`** — append-only deltas (`TRADE_BUY_FILL` / `TRADE_SELL_FILL`) with optional `execution_id` → `executions`.
- **`manual_settlement_actions`** — stub for four-eyes manual instructions (Beard Admin later).
- **`executions.settlement_status`** — enum `execution_settlement_status`; new fills default to **`executed`**. Transitions to `matched` / `confirmed` / … are not automated yet.

## Runtime behaviour

On each **applied** trade (after idempotent insert into `executions`), **`PositionsRepository.recordTradeFill`** updates **`positions`** + **`position_history`** using the custody account from **`oms.settlement.default-custody-account-id`** / **`OMS_SETTLEMENT_DEFAULT_CUSTODY_ACCOUNT_ID`** (must match a `custody_accounts.id`).

## Related configuration

See [configuration.md](configuration.md) — **Settlement / positions (slice 6)**.
