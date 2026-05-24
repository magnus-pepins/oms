# FIX outbound and inbound (Aeron Cluster substrate)

This document holds session config, `NewOrderSingle` / `ExecutionReport` field maps, environment
variables, and runbooks for **QuickFIX/J** wired to the cluster substrate via `oms-fix-egress`.

## Topology

After Phase 3 of the [Aeron Cluster substrate plan](../../system-documentation/plans/oms-aeron-cluster-substrate.md),
FIX wiring lives entirely on the **`oms-fix-egress`** JVM:

- **Outbound `NewOrderSingle`** is sent on `OrderAdmittedEvent` replay from the cluster's events
  recording. `OmsFixEgressService` opens an `AeronArchive.replay` from the persisted
  `oms_fix_egress_cursor` (Flyway V26), decodes each fragment, builds a `NewOrderSingle` via
  `FixNewOrderSingleBuilder.build(OrderAdmittedEvent)`, and sends through `FixOutboundSessionSend`.
  The cursor advances after `Session.sendToTarget` returns; on `SessionNotFound` (initiator mid-
  reconnect) the loop parks `oms.cluster.fix-egress.session-not-ready-park-nanos` and retries the
  same fragment without advancing.
- **Inbound `ExecutionReport` / `OrderCancelReject`** routes through `OmsFixApplication.fromApp` →
  `FixInboundClusterSink`, which translates the FIX message into `ApplyExecutionReportCommand` and
  submits via `OmsClusterIngressClient.submitApplyExecutionReport`. The cluster
  (`OmsAdmissionClusteredService`) walks the order state machine deterministically and emits
  `ExecutionAppliedEvent`; the projector consumes it and writes Postgres ([`return-path.md`](return-path.md)).
- **Cluster wire dedupe** is on `(senderCompId, msgSeqNum)` for FIX-origin commands, so QuickFIX/J
  resends after a session restart are no-ops at the cluster boundary. Combined with the cluster's
  `(orderId, venueExecRef)` index, every applied ER is exactly-once at the projector.

The legacy `FixRouteDispatcher` / `FixOutboundDispatchWorker` / `FixOutboundOrderDequeue` /
`FixOutboundTokenBucket` / `fix_nos_route_enqueue_claim` / `applyOutboundJobExpired` /
`FixInboundHandler` / `ExecutionReportApplier` were deleted across slices 3g / 3g-2.

## Tests

- **Smoke test:** `FixLogonSmokeTest` — embedded acceptor + initiator logon on a loopback port;
  includes **`reconnectCompletesSecondLogonWhenResetOnLogonEnabled`** with **`ResetOnLogon=Y`**
  on both sides (§14.6 day-rollover / seq-reset contract).
- **Outbound IT:** `OmsFixEgressBrokerIT` — boots `{test, oms-fix-egress}` with an embedded
  `EgressBrokerEmbeddedAcceptor` + `EgressBrokerCountingAcceptorApplication`; submits one
  `AcceptOrderCommand` via cluster, asserts exactly one NOS lands at the acceptor for that
  `orderId`; closes the autowired `OmsFixEgressService` and constructs a fresh instance with the
  same Spring deps + `init()` (simulates JVM restart of just the egress role); waits a quiet
  period and asserts no duplicate NOS for the previous `orderId`. Per-`orderId` UUID-keyed
  assertions so JVM-wide cluster recording backlog from earlier tests cannot interfere.
- **Round-trip IT:** `OmsFixEgressInboundErRoundTripIT` — submits one `AcceptOrderCommand` via
  the manually-wired ingress client, asserts the slice 3b-2 path delivers the NOS to a
  `EgressBrokerFillingAcceptorApplication` (counts NOS, replies with synthesised `PARTIAL_FILL
  ExecutionReport` carrying unique `ExecID` / `LastQty` / `LastPx`), re-submits the *same*
  `AcceptOrderCommand` (same idempotency key) and Awaitility-asserts the duplicate response carries
  `version=1` (proof the cluster applied the ER and bumped state).
- **Replay IT:** `OmsFixEgressReplayIT` — V26 cursor advances in lockstep with
  `OmsFixEgressService.lastAppliedPosition`.
- **Topology validator IT:** `OmsFixEgressApplicationIT` — context-only IT confirming the bean
  topology (`OmsFixEgressService` loads, `OrderIngressService` / `OmsPostgresProjector` are
  absent, `OmsClusterIngressClient` is present from slice 3d).
- **Mapper unit test:** `FixExecutionReportMapperTest` (FIX `ExecutionReport` /
  `OrderCancelReject` → `ExecutionTradeCommand` / `ExecutionCancelCommand` /
  `ExecutionVenueRejectCommand`).
- **Cluster sink unit test:** `FixInboundClusterSinkTest` (translation + scaling correctness +
  raw envelope JSON shape + unrecognised message types are no-ops).

## Configuration

| Key | Meaning |
|-----|---------|
| `OMS_ROUTING_BACKEND` | Set to **`fix`** to enable FIX beans on `oms-fix-egress`. |
| `OMS_FIX_AUTO_START` | **`true`** starts QuickFIX/J `SocketInitiator`; default **`false`** (safe for tests / bring-up without a broker). |
| `OMS_FIX_FILE_STORE_PATH` | QuickFIX/J file store directory (default `./queues/fix`). |
| `OMS_FIX_SOCKET_CONNECT_HOST` / `OMS_FIX_SOCKET_CONNECT_PORT` | Broker acceptor endpoint. |
| `OMS_FIX_SENDER_COMP_ID` / `OMS_FIX_TARGET_COMP_ID` | Session comp ids (defaults `OMS_INIT` / `BROKER_ACCEPT`). |
| `OMS_FIX_HEART_BT_INT` | Heartbeat interval (seconds). |
| `OMS_FIX_VENUE_ID_FOR_EXECUTIONS` | Venue id stamped on `ApplyExecutionReportCommand` from inbound ERs. |
| `OMS_FIX_USE_DATA_DICTIONARY` | `Y`/`N` passed to QuickFIX/J (`false` by default, matching smoke test). |
| `OMS_FIX_SYMBOL_MAP_JSON` | JSON object: OMS `instrument_symbol` key (matched case-insensitively) → broker **`Symbol`** on outbound `NewOrderSingle`. Empty / unset → identity mapping (`{}` in config). |
| `OMS_FIX_ROUTE_KEY` | Matches `fix_route_state.route_key` (default `default`). Gates `oms-fix-egress` outbound when `send_enabled=false`. |
| `OMS_FIX_SESSION_STORE_TYPE` | **`file`** (default) or **`jdbc`** — JDBC uses app `DataSource` by default; set **`OMS_FIX_SESSION_JDBC_DATASOURCE_ENABLED=true`** and **`OMS_FIX_SESSION_JDBC_*`** for a **second** Hikari pool. Apply Flyway **V9** to whichever database holds `oms_fix_*`. |
| `OMS_FIX_ROUTE_STATE_SOD_ENABLED` / `OMS_FIX_ROUTE_STATE_SOD_CRON` | Optional start-of-day: set all `fix_route_state.send_enabled=true` on cron (`FixRouteStateSodScheduler`). |
| `OMS_FIX_SOCKET_USE_SSL` / `OMS_FIX_SOCKET_*STORE*` / `OMS_FIX_ENABLED_SSL_PROTOCOLS` | Initiator TLS to broker (QuickFIX `SocketUseSSL`, keystores, `EnabledProtocols`). |
| `OMS_FIX_EGRESS_AERON_DIR` / `OMS_FIX_EGRESS_REPLAY_*` | Aeron Archive replay wiring for `oms-fix-egress` — see [configuration.md](configuration.md). |
| `OMS_FIX_EGRESS_CLUSTER_CLIENT_*` | Cluster ingress wiring for inbound ER → `ApplyExecutionReportCommand` (slice 3d). |

See `application-oms-fix-egress.yaml` (`oms.fix.*` + `oms.cluster.fix-egress.*`) and `.env.example`.
**Broker UAT soak (human checklist):** [fix-broker-uat-soak.md](fix-broker-uat-soak.md).

### Operator route setup (env + send gate)

FIX-out has **no** `oms_fix_out_session` catalog — configure the **`oms-fix-egress`** JVM env (`OMS_FIX_*`) and `fix_route_state.send_enabled`:

```bash
source ~/.oms-bench.env
cd ~/system-documentation

# Checklist + broker TCP probe + fix_route_state rows
bash scripts/oms-fix-out-route-setup.sh --inspect

# Print ecosystem/.env snippet for a new broker UAT session
bash scripts/oms-fix-out-route-setup.sh --print-env-snippet \
  --sender OMS_UAT --target BROKER_UAT --host broker.example --port 9876

# Dry-run then apply route send gate (Postgres)
bash scripts/oms-fix-out-route-setup.sh --send-enabled true --route-key default \
  --note "Broker UAT onboarded" --updated-by ops

bash scripts/oms-fix-out-route-setup.sh --commit --send-enabled true --route-key default \
  --note "Broker UAT onboarded" --updated-by ops
# then: pm2 restart oms-fix-egress  (after OMS_FIX_* env is set)
```

HTTP alternative (no script): `GET/PATCH /internal/v1/fix/route-state/{routeKey}` on `oms-ingress` (requires `X-OMS-Internal-Key`).

### Manual mass cancel (trading-ops / §9.3)

Internal HTTP **`POST /internal/v1/fix/mass-cancel-request`** (JSON: **`requestedBy`**, optional
**`reason`**, optional **`wire`**) is gated by **`OMS_FIX_MANUAL_MASS_CANCEL_ENABLED`**. Default
behaviour records the request and observability; venue **`MassCancelRequest`** wiring requires
**`OMS_FIX_MANUAL_MASS_CANCEL_WIRE_ENABLED=true`** and a **named broker** contract (same deferral
family as §14.5). After slice 3g-1 the wire send goes directly through `FixOutboundSessionSend`
(the legacy `FixRouteDispatcher` enqueue path is gone). Ops Console proxies this path as
**`POST /api/oms-trading/mass-cancel-request`** (**admin-only**).

### Mass cancel on disconnect (§14.5) — broker contract defer

When **`OMS_FIX_MASS_CANCEL_ON_DISCONNECT_ENABLED=true`**, OMS increments
**`oms_fix_mass_cancel_disconnect_signal_total`** and logs from
**`FixMassCancelOnDisconnectService`** on initiator **`onLogout`**. **Automated venue**
`OrderCancelRequest` / `MassCancelRequest` **fan-out is intentionally not implemented** until the
**named counterparty** documents: which open orders are in scope (e.g. `ClOrdID` = OMS order
UUID), behaviour on partial fills, and whether the broker expects a single mass message or
per-order cancels. Until then, ops follow manual kill / broker desk procedures after a disconnect
signal.

### ResetOnLogon (§14.6)

Broker day-rollover and test acceptors often require **`ResetOnLogon=Y`** (QuickFIX session
setting) so sequence numbers reset on each logon. Production values are broker-specific; the
smoke test above proves the QuickFIX/J stack tolerates a second logon after
**`SocketInitiator.stop()` / `start()`** when both sides enable reset-on-logon.

## Field mapping (v1)

- **Outbound `NewOrderSingle`** (built from `OrderAdmittedEvent` on the egress JVM): `ClOrdID` =
  order UUID string; `Symbol` = `FixSymbolMapper.toVenueSymbol(instrumentSymbol)` from
  **`oms.fix.symbol-map-json`** (identity when unmapped); `Side`; `OrderQty`; `OrdType` LIMIT if
  `limitPrice` set else MARKET; `Price` when limit; `TimeInForce` from order string (`DAY`
  default); `TransactTime` UTC.
- **Inbound `ExecutionReport`** (translated into `ApplyExecutionReportCommand` by
  `FixInboundClusterSink`): `ClOrdID` → order id (UUID); `ExecID` → `venue_exec_ref`;
  `TransactTime` → `venueTs` (fallback `Instant.now()`); **Fill / Partial fill:** `LastQty`,
  `LastPx` (optional → `0`), `LeavesQty`, `CumQty` → `EXEC_TYPE_TRADE`; **Canceled:**
  `ExecType=CANCELED` → `EXEC_TYPE_CANCEL`; **Rejected (new order):** `ExecType=Rejected` →
  `EXEC_TYPE_VENUE_REJECT` with `rejectCode=VENUE_REJECT`.
- **Inbound `OrderCancelReject` (9):** `OrigClOrdID` (else `ClOrdID`) → order id; `venue_exec_ref`
  = `ocr-{OrderID}-{CxlRejReason}`; `EXEC_TYPE_VENUE_REJECT` (same path as ER reject).
- **Cluster wire dedupe:** every inbound `ApplyExecutionReportCommand` carries the QuickFIX/J
  message `SenderCompID` + `MsgSeqNum`; `OmsAdmissionClusteredService` no-ops on
  `(senderCompId, msgSeqNum)` collisions so QuickFIX/J resends after session restart are
  silently dropped at the cluster boundary.

## Metrics

- `oms_fix_nos_sent_total` — successful outbound `Session.sendToTarget` on `oms-fix-egress`.
- `oms_fix_inbound_execution_reports_total{disposition=trade_*|cancel_*|venue_reject_*|ocr_*|ignored}`
  — every inbound ER routed through `FixInboundClusterSink`.
- `oms_fix_route_state_sod_reconciliations_total` — SOD job runs when enabled.
- `oms_fix_route_state_sod_skipped_total{reason}` — `FixSodPolicyEngine` gates the cron.
- `oms_executions_applied_total{outcome=inserted|duplicate}` — projector counter, one per applied
  ER.

The legacy `oms_fix_outbound_job_expired_total` / `oms_fix_outbound_route_disabled_skips_total` /
`oms_fix_outbound_throttled_requeues_total` counters were retired with the legacy outbound
dispatch worker in slice 3g-1.

## Local synthetic traffic (no real broker)

Use the same QuickFIX loopback pattern as `OmsFixEgressBrokerIT`, but as **two processes** so you
can drive HTTP load while watching **Micrometer** (`/actuator/prometheus`) or **OTel**
(`:9464/metrics` when `OMS_OTEL_METRICS_ENABLED=true`).

1. **Acceptor (terminal A)** — auto-replies to each **`D`** with a synthetic full-fill
   **`ExecutionReport`** (same handler as the IT, **`FixRoundTripAcceptorApplication`**):

   ```bash
   ./gradlew fixLoopbackAcceptor
   ```

   Defaults: listen **`9876`**, session **`SenderCompID=BROKER_ACCEPT`**,
   **`TargetCompID=OMS_INIT`** (must match OMS defaults **`oms.fix.sender-comp-id`** /
   **`oms.fix.target-comp-id`**). Override with **`FIX_ACCEPTOR_PORT`**,
   **`FIX_ACCEPTOR_SESSION_SENDER`**, **`FIX_ACCEPTOR_SESSION_TARGET`**,
   **`FIX_ACCEPTOR_FILE_STORE`** if your OMS env differs.

2. **OMS (terminal B)** — run an `oms-fix-egress` JVM (`./gradlew bootRunFixEgress` after
   `bootJarFixEgress`) with `OMS_ROUTING_BACKEND=fix`, **`OMS_FIX_AUTO_START=true`** (**required**
   — default is **`false`**, in which case **no `SocketInitiator` runs** and the loopback acceptor
   will never see a TCP session), **`OMS_FIX_SOCKET_CONNECT_HOST=127.0.0.1`**, port aligned with
   step 1, plus the cluster wiring (`OMS_FIX_EGRESS_AERON_DIR`, `OMS_FIX_EGRESS_CLUSTER_CLIENT_*`)
   pointing at a running cluster node. Confirm in OMS logs: **`FIX SocketInitiator started`**.
   For unrestricted symbols in dev, set **`OMS_RISK_INSTRUMENT_ALLOWLIST_ENABLED=false`** (or add
   your symbol to the allowlist). Ensure **`fix_route_state.send_enabled`** is **true** for your
   route key.

3. **HTTP shooter (terminal C)** — many **`POST /internal/v1/orders`** with unique idempotency
   keys (against an `oms-ingress-replica` or monolithic JVM):

   ```bash
   export OMS_INTERNAL_API_KEY=…
   ./scripts/benchmark/shoot-ingress-orders.sh   # SHOOT_COUNT=200 SHOOT_SLEEP_MS=10 env optional
   ```

See also **`scripts/benchmark/ingress-to-fix-nos-smoke.sh`** (single POST + optional OTel scrape
grep).

## Runbook notes

- With **`OMS_ROUTING_BACKEND=fix`** and **`OMS_FIX_AUTO_START=false`**, the egress JVM loads FIX
  beans but does **not** open a TCP session — useful for integration tests and staged deploys.
- Point `OMS_FIX_*` at a paper/test acceptor before production; keep `UseDataDictionary` aligned
  with the counterparty.
