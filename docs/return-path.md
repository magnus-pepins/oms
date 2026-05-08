# Return path (slice 3–4)

The **return path** applies venue execution reports (ER-shaped messages) to Postgres:
`executions` rows (idempotent on `(account_id, venue_exec_ref)`), `orders.cum_filled_quantity`,
**`positions`** / **`position_history`** on each applied **trade** (slice 6 — default custody;
see **`docs/settlement.md`**), **`executions.settlement_status`** (defaults to **`executed`**;
broker-driven transitions later), and domain fanout envelopes (`OrderPartiallyFilled`,
`OrderFilled`, `OrderCancelled`).

**Slice 4 (FIX):** venue **rejects** (`ExecutionReport` `ExecType=Rejected`, `OrderCancelReject`) and **outbound job expiry** map to `executions.exec_type=REJECT`, `orders` → `REJECTED` with `terminal_reason` `VENUE_REJECT` or `FIX_OUTBOUND_JOB_EXPIRED`, and **`OrderRejected`** in `domain_event_outbox` (same envelope shape as control rejects). See `docs/fix-out.md`.

## Components

| Piece | Role |
|-------|------|
| `ExecutionReportApplier` | `@Transactional` apply for trades, cancels, **venue rejects**, and **outbound job expiry**; merges **`OMS_MARKET_CONTEXT_STUB_JSON`** with venue-attested fields into `market_context.snapshot_json` on each **trade** apply (NBBO/marketdata later). After each successful trade insert, **`PositionsRepository.recordTradeFill`** updates **`positions`** + **`position_history`** using **`oms.settlement.default-custody-account-id`** ([`docs/settlement.md`](settlement.md)). |
| `ExecutionsRepository` | `ON CONFLICT (account_id, venue_exec_ref) DO NOTHING` for idempotency; `exec_type` **TRADE** / **CANCEL** / **REJECT**. |
| `RouteDispatcher` | Called from `ControlTailer` **after commit** when an order reaches `WORKING`. |
| `SimulatedBrokerDispatcher` | When `OMS_ROUTING_BACKEND=simulated`: enqueues order ids on the simulated route queue. |
| `SimulatedReturnPathProjectionWorker` | `@Scheduled` drain (or `processPendingQueueOnce()` in tests) → `SimulatedExecutionProgram`. |
| `SimulatedExecutionProgram` | Emits three synthetic ER applies (⅓, ⅓, remainder) → typically two `OrderPartiallyFilled` then `OrderFilled`. |
| `FixRouteDispatcher` | When `OMS_ROUTING_BACKEND=fix`: queues ids for QuickFIX/J outbound; see `docs/fix-out.md` for initiator, NOS, inbound ER/OCR, stale dequeue. |

## Configuration

See `docs/configuration.md` (`oms.routing.*`) and `.env.example`.

- **`OMS_ROUTING_BACKEND=noop`** (default): no post-`WORKING` dispatch.
- **`OMS_ROUTING_BACKEND=simulated`**: `SimulatedBrokerDispatcher` + `SimulatedReturnPathProjectionWorker` + scheduled drain (`OMS_SIMULATED_POLL_INTERVAL_MS`). Tests set `OMS_SIMULATED_SCHEDULER_ENABLED=false` and call **`processPendingQueueOnce()`**.
- **`OMS_ROUTING_BACKEND=fix`**: `FixRouteDispatcher` (see `docs/fix-out.md`).

## Metrics

- `oms_executions_applied_total{outcome=inserted|duplicate}`
- `oms_order_filled_events_published_total`

## Next (broker hardening)

Per master plan: optional dedicated FIX session JDBC URL, broker UAT soak — see `docs/fix-out.md` and `docs/fix-broker-uat-soak.md`.

**Settlement (slice 6, in progress):** schema + fill→positions wiring is in **`docs/settlement.md`**. Still open: broker confirms / `settlement_status` transitions beyond **`executed`**, reconciliation jobs, Ledger cash legs on settle.
