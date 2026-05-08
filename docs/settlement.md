# Settlement spine (slice 6 — Phase 1 §12.2–12.3)

This document describes the **securities post-trade** tables and wiring in OMS. **Ledger** cash legs, broker EOD file formats, and Beard Admin four-eyes flows remain plan-driven.

## Schema

### Flyway `V11__settlement_positions.sql`

- **`custody_accounts`** — broker custody routing (Shape A v1). A deterministic **DEFAULT** omnibus row is inserted for single-broker wiring.
- **`positions`** — one row per `(account_id, instrument_symbol, custody_account_id)` with quantity columns (`quantity_total`, `quantity_settled`, `quantity_pending_buy_settle`, `quantity_pending_sell_settle`).
- **`position_history`** — append-only deltas (`TRADE_BUY_FILL` / `TRADE_SELL_FILL`, **`SETTLEMENT_BUY_SETTLED`**) with optional `execution_id` → `executions`.
- **`manual_settlement_actions`** — stub for four-eyes manual instructions (Beard Admin later).
- **`executions.settlement_status`** — enum `execution_settlement_status`; new fills default to **`executed`**.

### Flyway `V12__broker_settlement_confirm.sql`

- **`broker_settlement_confirm`** — one row per **`execution_id`** (unique). **`applied_at`** null = pending; **`SettlementConfirmProcessor`** / internal HTTP drain the queue and run the §12.3 chain to **`settled`**.

## State machine (§12.3)

Forward-only path persisted on **`executions`**:

`executed` → `matched` → `confirmed` → `settling` → `settled` (and **`failed`** reserved for later).

- **`SettlementConfirmProcessor`** advances **`TRADE`** rows. **SELL** legs advance status only (no position move on settle in this slice).
- On transition into **`settled`** for **BUY**, **`PositionsRepository.recordBuySettled`** moves quantity from **`quantity_pending_buy_settle`** to **`quantity_settled`** and appends **`SETTLEMENT_BUY_SETTLED`** history.

## Runtime behaviour

### Trade apply (return path)

On each **applied** trade (after idempotent insert into `executions`), **`PositionsRepository.recordTradeFill`** updates **`positions`** + **`position_history`** using **`oms.settlement.default-custody-account-id`** / **`OMS_SETTLEMENT_DEFAULT_CUSTODY_ACCOUNT_ID`** (must match a `custody_accounts.id`).

### Read-only inspection (internal HTTP)

- **`GET /internal/v1/settlement/executions`** — paginated list: `executions` inner join `orders`; requires **`orderId`** or both **`from`** and **`to`** (half-open on `executions.created_at`); optional **`settlementStatus`**; **`limit`** / **`offset`** capped (same pattern as **`GET /internal/v1/control-decisions`**).
- **`GET /internal/v1/settlement/executions/{id}`** — single row with order fields plus **`rawEnvelopeJson`** (venue envelope). **404** if missing.

### Broker confirm queue

1. **`POST /internal/v1/settlement/broker-confirms`** — body `{ "executionIds": [ … ] }` (max size from **`OMS_SETTLEMENT_BROKER_CONFIRM_HTTP_MAX_EXECUTION_IDS`**); inserts pending rows (`ON CONFLICT DO NOTHING`).
2. **`POST /internal/v1/settlement/process-pending`** — optional query `maxBatch`; locks up to **`broker-confirm-reconciler-batch-size`** pending rows with **`FOR UPDATE SKIP LOCKED`**, runs the full pipeline per row, sets **`applied_at`**.
3. **`POST /internal/v1/settlement/executions/{id}/advance-one-step`** — advances exactly one enum step (tests / manual ops).

All routes require **`X-OMS-Internal-Key`** (see [decisions.md](decisions.md)).

Optional **`BrokerSettlementConfirmScheduler`** runs when **`OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_ENABLED=true`** on **`OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_INTERVAL_MS`**. Metric **`oms_settlement_broker_confirm_processed_total`**.

### Test fixture

`src/test/resources/settlement/broker-confirm-request.template.json` — template JSON with placeholder **`-1`** in **`executionIds`** (replaced in IT with a real **`executions.id`**).

## Related configuration

See [configuration.md](configuration.md) — **Settlement / positions (slice 6)**.
