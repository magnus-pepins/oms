# Settlement spine (slice 6 — Phase 1 §12.2–12.3)

This document describes the **securities post-trade** tables and wiring in OMS. **Ledger** cash legs, broker EOD file formats, and Beard Admin four-eyes flows remain plan-driven. **Phase 1 breaks dry-run (ops):** [settlement-breaks-dry-run-runbook.md](settlement-breaks-dry-run-runbook.md).

## Schema

### Flyway `V11__settlement_positions.sql`

- **`custody_accounts`** — broker custody routing (Shape A v1). A deterministic **DEFAULT** omnibus row is inserted for single-broker wiring.
- **`positions`** — one row per `(account_id, instrument_symbol, custody_account_id)` with quantity columns (`quantity_total`, `quantity_settled`, `quantity_pending_buy_settle`, `quantity_pending_sell_settle`).
- **`position_history`** — append-only deltas (`TRADE_BUY_FILL` / `TRADE_SELL_FILL`, **`SETTLEMENT_BUY_SETTLED`**, **`SETTLEMENT_SELL_SETTLED`**, **`MARK_FAILED_UNWIND_BUY`** / **`MARK_FAILED_UNWIND_SELL`**) with optional `execution_id` → `executions`.
- **`manual_settlement_actions`** — four-eyes manual instructions (§12.8); **`POST /internal/v1/settlement/manual-actions`** + approve route (see Read-only / Manual below).
- **`executions.settlement_status`** — enum `execution_settlement_status`; new fills default to **`executed`**.

### Flyway `V12__broker_settlement_confirm.sql`

- **`broker_settlement_confirm`** — one row per **`execution_id`** (unique). **`applied_at`** null = pending; **`SettlementConfirmProcessor`** / internal HTTP drain the queue and run the §12.3 chain to **`settled`**.

### Flyway `V13__execution_sell_fill_position_split.sql`

- **`executions.sell_position_from_pending_buy`** / **`sell_position_from_settled`** — for **SELL** **`TRADE`** rows, how the fill was sourced from **`positions`** (exact unwind on operator **`mark-failed`**). Written in the same transaction as the fill (**`ExecutionReportApplier`** + **`PositionsRepository.recordTradeFill`**).

### Flyway `V14__settlement_file_import_batch.sql`

- **`settlement_file_import_batch`** — idempotency ledger for **broker file** ingest (SHA-256 unique); v1 **`POST /internal/v1/settlement/file-import`** (multipart JSON, same **`rows`** shape as **`import-json`**) is described in [settlement-eod-ingest.md](settlement-eod-ingest.md). **`POST …/broker-confirms/import-json`** remains convenient for dev/UAT without multipart.

### Flyway `V16__ledger_settlement_outbox.sql`

- **`ledger_settlement_outbox`** — optional enqueue when **`OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED=true`** in the same transaction as **`settled`** on **`TRADE`** rows; optional drain when **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true`** (see [settlement-ledger-posting.md](settlement-ledger-posting.md)).

## State machine (§12.3)

Forward-only path persisted on **`executions`**:

`executed` → `matched` → `confirmed` → `settling` → `settled` (and **`failed`** reserved for later).

- **`SettlementConfirmProcessor`** advances **`TRADE`** rows.
- On transition into **`settled`** for **BUY**, **`PositionsRepository.recordBuySettled`** moves quantity from **`quantity_pending_buy_settle`** to **`quantity_settled`** and appends **`SETTLEMENT_BUY_SETTLED`** history.
- **SELL** fills increment **`quantity_pending_sell_settle`** (same trade-apply txn as **`TRADE_SELL_FILL`**). On transition into **`settled`**, **`PositionsRepository.recordSellSettled`** decrements **`quantity_pending_sell_settle`** and appends **`SETTLEMENT_SELL_SETTLED`** history.

## Runtime behaviour

### Trade apply (return path)

On each **applied** trade (after idempotent insert into `executions`), **`PositionsRepository.recordTradeFill`** updates **`positions`** + **`position_history`** using **`oms.settlement.default-custody-account-id`** / **`OMS_SETTLEMENT_DEFAULT_CUSTODY_ACCOUNT_ID`** (must match a `custody_accounts.id`). **SELL** fills also persist **`sell_position_from_*`** on **`executions`** for mark-failed unwind.

### Operator mark-failed (`POST …/executions/{id}/mark-failed`)

Clears pending **`broker_settlement_confirm`** rows, moves the **`TRADE`** **`settlement_status`** to **`failed`**, and **unwinds `positions`** for that execution’s fill when possible: **BUY** always reverses **`quantity_total`** / **`quantity_pending_buy_settle`** (appends **`MARK_FAILED_UNWIND_BUY`** history). **SELL** reverses when **`sell_position_from_*`** were stored at fill time (appends **`MARK_FAILED_UNWIND_SELL`**); legacy SELL rows without splits log a skip and leave **`positions`** unchanged (operator must correct manually).

### Read-only inspection (internal HTTP)

- **`GET /internal/v1/settlement/executions`** — paginated list: `executions` inner join `orders`; requires **`orderId`** or both **`from`** and **`to`** (half-open on `executions.created_at`); optional **`settlementStatus`**; **`limit`** / **`offset`** capped (same pattern as **`GET /internal/v1/control-decisions`**).
- **`GET /internal/v1/settlement/executions/{id}`** — single row with order fields plus **`rawEnvelopeJson`** (venue envelope). **404** if missing.
- **`POST /internal/v1/settlement/manual-actions`** — stage a row (`execution_id`, `action_type`, `requested_by`, `payload_json`); execution must exist (**404** if not).
- **`GET /internal/v1/settlement/manual-actions`** — paginated list; requires **`executionId`** or both **`from`** and **`to`** on `created_at`.
- **`GET /internal/v1/settlement/file-import-batches`** — paginated **`settlement_file_import_batch`** rows (**`limit`** / **`offset`**; caps from **`OMS_SETTLEMENT_FILE_IMPORT_LIST_*`**).
- **`GET /internal/v1/settlement/manual-actions/{id}`** — single staged/approved row. **404** if missing.
- **`POST /internal/v1/settlement/manual-actions/{id}/approve`** — sets **`approved_by`** when null; approver must differ from **`requested_by`** (case-insensitive). **409** if already approved or concurrent loss; **400** if same actor. When **`OMS_SETTLEMENT_MANUAL_ACTION_AUTO_APPLY_ENABLED`** is **`true`** (default), supported **`action_type`** values run in the **same** Postgres transaction as the approve CAS: **`MARK_TRADE_FAILED`** → **`SettlementConfirmProcessor.markTradeFailed`**; **`ADVANCE_SETTLEMENT_ONE_STEP`** → **`advanceOneSettlementStep`**; **`REGISTER_BROKER_CONFIRM`** → **`enqueueBrokerSettlementConfirmForTradeOrThrow`** (pending **`broker_settlement_confirm`** row; idempotent); **`CLEAR_PENDING_BROKER_CONFIRM`** → **`clearPendingBrokerConfirmsForTradeOrThrow`** (deletes **pending** queue rows only; does not change **`settlement_status`**). Other types are approved only (audit / future processors). On apply failure the whole transaction (including approve) rolls back.

### Broker confirm queue

1. **`POST /internal/v1/settlement/broker-confirms`** — body `{ "executionIds": [ … ] }` (max size from **`OMS_SETTLEMENT_BROKER_CONFIRM_HTTP_MAX_EXECUTION_IDS`**); inserts pending rows (`ON CONFLICT DO NOTHING`).
2. **`POST /internal/v1/settlement/process-pending`** — optional query `maxBatch`; locks up to **`broker-confirm-reconciler-batch-size`** pending rows with **`FOR UPDATE SKIP LOCKED`**, runs the full pipeline per row, sets **`applied_at`**.
3. **`POST /internal/v1/settlement/executions/{id}/advance-one-step`** — advances exactly one enum step (tests / manual ops).
4. **`POST /internal/v1/settlement/broker-confirms/import-json`** — body `{ "rows": [ … ] }` with **`BrokerFixtureRow`** shape (explicit **`executionId`** or **`accountId` + `venueExecRef`**); same resolution rules as **`file-import`**.
5. **`POST /internal/v1/settlement/file-import`** — `multipart/form-data` with **`source`** (text) and **`file`** (UTF-8 JSON body `{"rows":[…]}`); computes SHA-256, inserts **`settlement_file_import_batch`** (`ON CONFLICT` skip = duplicate), registers **`broker_settlement_confirm`** in slices (**`OMS_SETTLEMENT_FILE_IMPORT_REGISTER_SLICE_SIZE`**). **413** when file exceeds **`OMS_SETTLEMENT_FILE_IMPORT_MAX_BYTES`**. Response includes **`duplicate`**, **`batchId`**, **`status`** (`applied` / `failed` / prior status on duplicate).

All routes require **`X-OMS-Internal-Key`** (see [decisions.md](decisions.md)).

Optional **`BrokerSettlementConfirmScheduler`** runs when **`OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_ENABLED=true`** on **`OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_INTERVAL_MS`**. Metric **`oms_settlement_broker_confirm_processed_total`**.

### Test fixture

`src/test/resources/settlement/broker-confirm-request.template.json` — template JSON with placeholder **`-1`** in **`executionIds`** (replaced in IT with a real **`executions.id`**).

## Related configuration

See [configuration.md](configuration.md) — **Settlement / positions (slice 6)**.

## Broker file contract

Formal JSON row model, idempotency, and transport matrix: [broker-eod-file-contract.md](broker-eod-file-contract.md).
