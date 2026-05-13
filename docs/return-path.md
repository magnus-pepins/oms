# Return path (Aeron Cluster substrate)

The **return path** applies venue execution reports (ER-shaped messages) to Postgres: `executions`
rows (idempotent on `(account_id, venue_exec_ref)`), `orders.cum_filled_quantity`, `orders.status`
(`PARTIALLY_FILLED` / `FILLED` / `CANCELLED` / `REJECTED`), **`positions`** / **`position_history`**
on each applied **trade** (default custody account; see [`settlement.md`](settlement.md)),
**`executions.settlement_status`** (defaults to **`executed`**; broker-driven transitions in
[`settlement.md`](settlement.md)), and domain fanout envelopes (`OrderPartiallyFilled`,
`OrderFilled`, `OrderCancelled`, `OrderRejected`).

After Phase 3 of the [Aeron Cluster substrate plan](../../system-documentation/plans/oms-aeron-cluster-substrate.md):

- The **cluster** (`OmsAdmissionClusteredService`) owns the order state machine. It consumes
  `ApplyExecutionReportCommand` (typeId 2 on the cluster ingress channel), walks the deterministic
  state transition (TRADE / CANCEL / VENUE_REJECT — see [`adr/0001-aeron-cluster-substrate.md`](adr/0001-aeron-cluster-substrate.md)),
  emits `ExecutionAppliedEvent` (typeId 1003) on the events recording, and dedupes on
  `(orderId, venueExecRef)` plus `(senderCompId, msgSeqNum)` for FIX-origin commands.
- **`OmsPostgresProjector`** consumes the events recording via Aeron Archive replay. For each
  `ExecutionAppliedEvent` it inserts the `executions` row, CASes the `orders` row to the
  post-apply state, merges venue evidence into `market_context.snapshot_json`, calls
  `PositionsRepository.recordTradeFill` (BUY / SELL split), appends free-riding attribution links
  (when `oms.settlement.free-riding-attribution-enabled=true`), and writes the matching
  `domain_event_outbox` envelope — all in one Postgres transaction. The projector cursor
  (`aeron_projector_cursor`) advances byte-position-by-byte-position so a restart picks up exactly
  where the prior transaction committed.
- **`oms-fix-egress`** translates inbound QuickFIX/J `ExecutionReport` / `OrderCancelReject` into
  `ApplyExecutionReportCommand` and submits via the same `OmsClusterIngressClient` ingress used by
  order accept. Outbound `NewOrderSingle` is sent on `OrderAdmittedEvent` replay. See
  [`fix-out.md`](fix-out.md).

The Phase 1–2 legacy components (`ControlTailer`, `RouteDispatcher`, `SimulatedBrokerDispatcher`,
`SimulatedReturnPathProjectionWorker`, `SimulatedExecutionProgram`, `FixRouteDispatcher`,
`FixOutboundDispatchWorker`, `ExecutionReportApplier`, `control_outbox` + `OutboxReconciler`,
`fix_nos_route_enqueue_claim`, the entire Chronicle module, `oms-control-worker` /
`oms-fix-worker` JVMs) were deleted across slices 3f / 3g / 3g-2.

## Components

| Piece | Role |
|-------|------|
| `OmsAdmissionClusteredService` | Cluster service consuming `AcceptOrderCommand` + `ApplyExecutionReportCommand`; emits `OrderAdmittedEvent` + `ExecutionAppliedEvent` on the events recording. Authoritative order state lives here (snapshot v3). |
| `OmsPostgresProjector` | Consumes the events recording via Aeron Archive replay; writes `executions` / `orders` / `market_context` / `positions` / `position_history` / `domain_event_outbox` in one TX per fragment. Advances `aeron_projector_cursor` last. |
| `ExecutionsRepository.tryInsertTrade / tryInsertCancel / tryInsertVenueReject` | `ON CONFLICT (account_id, venue_exec_ref) DO NOTHING` for idempotency; returns `Optional.empty()` on duplicate so the projector skips downstream side effects. |
| `OrdersRepository.updateFillOrCancelWithCas / updateWithCas` | CAS on Postgres `orders.version` after each ER apply. The cluster's in-memory version and Postgres version are intentionally not in lockstep (Postgres bumps once at admission for `WORKING`, then once per applied ER); the projector reads the current Postgres version inside the same TX. |
| `MarketContextVenueEvidence` | Builds the venue-attested JSON patch the projector merges into `market_context.snapshot_json` on every TRADE; same shape the legacy applier used. |
| `OmsFixEgressService` (`oms-fix-egress`) | Reads cluster events recording via Aeron Archive replay; sends `NewOrderSingle` on each `OrderAdmittedEvent`. Cursor in `oms_fix_egress_cursor`. |
| `FixInboundClusterSink` (`oms-fix-egress`) | Translates inbound QuickFIX/J `ExecutionReport` / `OrderCancelReject` into `ApplyExecutionReportCommand` and submits via `OmsClusterIngressClient.submitApplyExecutionReport`. |

## Configuration

See [`configuration.md`](configuration.md) (`oms.routing.*`, `oms.cluster.*`,
`oms.fix.*`) and `.env.example`.

- **`OMS_ROUTING_BACKEND=noop`** (default): no FIX outbound. `oms-fix-egress` will not load
  QuickFIX/J beans.
- **`OMS_ROUTING_BACKEND=fix`**: QuickFIX/J initiator on `oms-fix-egress` (see [`fix-out.md`](fix-out.md)).
- **Settlement:** internal **`/internal/v1/settlement/**`** for broker-confirm ingest and §12.3
  advances — see [`settlement.md`](settlement.md).

## Metrics

- `oms_executions_applied_total{outcome=inserted|duplicate}` — projector counter, one per applied
  `ExecutionAppliedEvent`.
- `oms_order_filled_events_published_total` — projector counter, one per `OrderFilled` envelope.
- `oms_free_riding_attribution_merges_total` — projector counter, one per second-buy attribution
  link.
- `oms.trade.apply` — projector timer wrapping `applyTradeProjectionTimed` (executions insert +
  market context + positions + free-riding + order CAS + domain outbox).
