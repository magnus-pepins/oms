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
| `SimulatedFillEngine` | When `OMS_ROUTING_BACKEND=simulated`, enqueues order ids and emits three synthetic partial fills (⅓, ⅓, remainder) at the order’s `limit_price` (or `1` if null). |

## Configuration

See `docs/configuration.md` (`oms.routing.*`) and `.env.example`.

- **`OMS_ROUTING_BACKEND=noop`** (default): no simulated fills; orders stay `WORKING` until a real broker lands in slice 4.
- **`OMS_ROUTING_BACKEND=simulated`**: enables `SimulatedFillEngine` + scheduled drain (`OMS_SIMULATED_POLL_INTERVAL_MS`). Tests set `OMS_SIMULATED_SCHEDULER_ENABLED=false` and call `drainOnceForTests()`.

## Metrics

- `oms_executions_applied_total{outcome=inserted|duplicate}`
- `oms_order_filled_events_published_total`

## Next (slice 4)

`OMS_ROUTING_BACKEND=fix` with QuickFIX/J replaces the simulated engine; the applier contract stays the same.
