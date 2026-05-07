# Return path (slice 3)

The **return path** applies venue execution reports (ER-shaped messages) to Postgres:
`executions` rows (idempotent on `(account_id, venue_exec_ref)`), `orders.cum_filled_quantity`,
and domain fanout envelopes (`OrderPartiallyFilled`, `OrderFilled`, `OrderCancelled`).

## Components

| Piece | Role |
|-------|------|
| `ExecutionReportApplier` | `@Transactional` apply for trades and cancels; inserts `market_context` stub on first trade. |
| `ExecutionsRepository` | `ON CONFLICT (account_id, venue_exec_ref) DO NOTHING` for idempotency. |
| `RouteDispatcher` | Called from `ControlTailer` **after commit** when an order reaches `WORKING`. |
| `SimulatedBrokerDispatcher` | When `OMS_ROUTING_BACKEND=simulated`: enqueues order ids on the simulated route queue. |
| `SimulatedReturnPathProjectionWorker` | `@Scheduled` drain (or `processPendingQueueOnce()` in tests) → `SimulatedExecutionProgram`. |
| `SimulatedExecutionProgram` | Emits three synthetic ER applies (⅓, ⅓, remainder) → typically two `OrderPartiallyFilled` then `OrderFilled`. |
| `FixRouteDispatcher` | When `OMS_ROUTING_BACKEND=fix`: queues ids for QuickFIX/J outbound (slice 4; initiator wiring next). |

## Configuration

See `docs/configuration.md` (`oms.routing.*`) and `.env.example`.

- **`OMS_ROUTING_BACKEND=noop`** (default): no post-`WORKING` dispatch.
- **`OMS_ROUTING_BACKEND=simulated`**: `SimulatedBrokerDispatcher` + `SimulatedReturnPathProjectionWorker` + scheduled drain (`OMS_SIMULATED_POLL_INTERVAL_MS`). Tests set `OMS_SIMULATED_SCHEDULER_ENABLED=false` and call **`processPendingQueueOnce()`**.
- **`OMS_ROUTING_BACKEND=fix`**: `FixRouteDispatcher` (see `docs/fix-out.md`).

## Metrics

- `oms_executions_applied_total{outcome=inserted|duplicate}`
- `oms_order_filled_events_published_total`

## Next (slice 4)

`OMS_ROUTING_BACKEND=fix` with QuickFIX/J (`FixRouteDispatcher` today; initiator + `fromApp` → applier next) replaces the simulated path for production; the applier contract stays the same.
