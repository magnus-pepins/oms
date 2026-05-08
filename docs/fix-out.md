# FIX outbound and inbound (slice 4+)

This document holds session config, `NewOrderSingle` / `ExecutionReport` field maps,
environment variables, and runbooks for **QuickFIX/J** wired to `FixRouteDispatcher` and
`ExecutionReportApplier`.

## Current state (slice 4 wiring)

- **Dependencies:** `org.quickfixj:quickfixj-core` + `quickfixj-messages-fix44` (see `build.gradle.kts`).
- **Smoke test:** `FixLogonSmokeTest` — embedded acceptor + initiator logon on a loopback port.
- **Spring IT:** `FixRoutingSpringIntegrationTest` — `oms.routing.backend=fix`, `oms.fix.auto-start=false`, asserts `RouteDispatcher` is `FixRouteDispatcher`.
- **Round-trip IT:** `FixRoundTripSpringIntegrationTest` (profile `fix-roundtrip-it`) — embedded loopback **acceptor** (`FixRoundTripEmbeddedAcceptor`, `SmartLifecycle` phase before initiator) + `oms.fix.auto-start=true`; inserts `WORKING` order, `RouteDispatcher.enqueueWorkingOrder`, awaits **FILLED** and asserts **`oms_fix_nos_sent_total`** / **`oms_fix_inbound_execution_reports_total`** (`disposition=trade_APPLIED`).
- **HTTP ingress → FIX round-trip IT:** `FixIngressRoundTripSpringIntegrationTest` — same profile; **`POST /internal/v1/orders`** → outbox reconciler → **`ControlTailer.apply`** → **`WORKING`** → NOS → synthetic fill → **FILLED**; asserts **`positions`** / **`position_history`** (slice 6) and **broker-confirm drain → `settled`** via shared test assertions.
- **Symbol-map round-trip IT:** `FixRoundTripSymbolMapSpringIntegrationTest` — same profile; **`oms.fix.symbol-map-json`** maps OMS `instrument_symbol` → broker `Symbol` on wire; order row keeps client symbol.
- **JDBC session store IT:** `FixRoundTripJdbcStoreSpringIntegrationTest` — same profile with `oms.fix.session-store-type=jdbc`; asserts **`oms_fix_sessions`** row after fill (Flyway **V9**).
- **Isolated FIX session Postgres (local):** Compose profile **`with-fix-session-db`** + runbook [fix-session-store-isolation.md](fix-session-store-isolation.md) (plan §6.4).
- **Dedicated session JDBC pool IT:** `FixRoundTripJdbcDedicatedSessionPoolSpringIntegrationTest` — `oms.fix.session-jdbc-datasource-enabled=true` with a second Hikari pool (IT points at the same Postgres as wiring proof).
- **Route-disabled outbound IT:** `FixOutboundRouteDisabledSpringIntegrationTest` — `send_enabled=false` → no NOS, **`oms_fix_outbound_route_disabled_skips_total`**, order stays **WORKING**; after re-enable, **FILLED** as usual.
- **Stale outbound IT:** `FixOutboundStaleSpringIntegrationTest` — same profile; `oms.fix.max-outbound-job-age-ms` bound; old `accepted_at` → dequeue rejects with **`FIX_OUTBOUND_JOB_EXPIRED`** (no NOS to acceptor; **`oms_fix_outbound_job_expired_total`**).
- **Return-path IT:** `VenueRejectReturnPathIntegrationTest` — `ExecutionReportApplier.applyVenueReject` → `REJECT` execution + **`OrderRejected`**.
- **Outbound:** `FixRouteDispatcher` enqueues `WORKING` order ids into a bounded `BlockingQueue` (capacity `oms.fix.outbound-queue-capacity`). When `oms.fix.auto-start=true`, `FixInitiatorManager` starts a `SocketInitiator` and `FixOutboundDispatchWorker` runs on `oms.fix.outbound-poll-interval-ms`. The worker **does not dequeue** while `fix_route_state.send_enabled` is false for `oms.fix.route-key` (orders stay queued). After dequeue, `FixOutboundTokenBucket` may **re-queue** the id when `oms.fix.outbound-tokens-per-second` &gt; `0` and the bucket is empty. Then it loads the order, builds `NewOrderSingle` (`FixNewOrderSingleBuilder`), and `Session.sendToTarget` on the active session from `FixSessionRegistry`.
- **Route state IT:** `FixRouteStateControllerIntegrationTest` — Flyway `fix_route_state`; GET/PATCH with `X-OMS-Internal-Key`; 401/404.
- **Inbound:** `OmsFixApplication` routes **`ExecutionReport`** and **`OrderCancelReject`** in `fromApp` → `FixInboundHandler` (`@Transactional`) → `FixExecutionReportMapper` → `ExecutionReportApplier` for **PartialFill/Fill**, **Canceled**, **Rejected** (`ExecType=Rejected` → `OrderRejected` + `executions` **REJECT**), and **OrderCancelReject** (same venue-reject path).
- **Outbound stale jobs:** when `oms.fix.max-outbound-job-age-ms` &gt; `0`, a **WORKING** order older than that at dequeue is **not** sent; `ExecutionReportApplier.applyOutboundJobExpired` CAS to **`REJECTED`** / **`FIX_OUTBOUND_JOB_EXPIRED`** (metric **`oms_fix_outbound_job_expired_total`**).
- **Metrics:** `oms_fix_nos_sent_total` (successful outbound `sendToTarget`); `oms_fix_inbound_execution_reports_total` with tag `disposition` = `trade_*` / `cancel_*` / `venue_reject_*` / `ocr_*` / `ignored`; **`oms_fix_outbound_job_expired_total`**; **`oms_fix_outbound_route_disabled_skips_total`** (scheduler ticks with logon while send is disabled); **`oms_fix_outbound_throttled_requeues_total`** (token bucket re-queue); **`oms_fix_route_state_sod_reconciliations_total`** (SOD job runs when enabled).
- **Mapper unit test:** `FixExecutionReportMapperTest`.

## Configuration

| Key | Meaning |
|-----|---------|
| `OMS_ROUTING_BACKEND` | Set to **`fix`** to enable FIX routing beans (`FixRouteDispatcher`, inbound/outbound stack). |
| `OMS_FIX_AUTO_START` | **`true`** starts `SocketInitiator` + outbound scheduler; default **`false`** (safe for tests / bring-up without a broker). |
| `OMS_FIX_OUTBOUND_QUEUE_CAPACITY` | Bounded queue for outbound order ids (default `10000`). |
| `OMS_FIX_FILE_STORE_PATH` | QuickFIX/J file store directory (default `./queues/fix`). |
| `OMS_FIX_SOCKET_CONNECT_HOST` / `OMS_FIX_SOCKET_CONNECT_PORT` | Broker acceptor endpoint. |
| `OMS_FIX_SENDER_COMP_ID` / `OMS_FIX_TARGET_COMP_ID` | Session comp ids (defaults `OMS_INIT` / `BROKER_ACCEPT`). |
| `OMS_FIX_HEART_BT_INT` | Heartbeat interval (seconds). |
| `OMS_FIX_OUTBOUND_POLL_INTERVAL_MS` | Scheduler delay between outbound drain attempts. |
| `OMS_FIX_MAX_OUTBOUND_JOB_AGE_MS` | **0** = off. Otherwise reject **WORKING** orders at FIX dequeue when `now - accepted_at` exceeds this (ms); **`terminal_reason=FIX_OUTBOUND_JOB_EXPIRED`**. |
| `OMS_FIX_VENUE_ID_FOR_EXECUTIONS` | Venue id on `ExecutionTradeCommand` / cancel from inbound ERs. |
| `OMS_FIX_USE_DATA_DICTIONARY` | `Y`/`N` passed to QuickFIX/J (`false` by default, matching smoke test). |
| `OMS_FIX_SYMBOL_MAP_JSON` | JSON object: OMS `instrument_symbol` key (matched case-insensitively) → broker **`Symbol`** on outbound `NewOrderSingle`. Empty / unset → identity mapping (`{}` in config). |
| `OMS_FIX_ROUTE_KEY` | Matches `fix_route_state.route_key` (default `default`). |
| `OMS_FIX_OUTBOUND_TOKENS_PER_SECOND` | **0** = unlimited NOS rate; **&gt; 0** enables token-bucket pacing (`OMS_FIX_OUTBOUND_TOKEN_BURST` caps burst). |
| `OMS_FIX_OUTBOUND_TOKEN_BURST` | Max tokens in bucket when rate limiting is enabled (default `100`). |
| `OMS_FIX_SESSION_STORE_TYPE` | **`file`** (default) or **`jdbc`** — JDBC uses app `DataSource` by default; set **`OMS_FIX_SESSION_JDBC_DATASOURCE_ENABLED=true`** and **`OMS_FIX_SESSION_JDBC_*`** for a **second** Hikari pool (plan 6.4). Apply Flyway **V9** to whichever database holds `oms_fix_*`. |
| `OMS_FIX_ROUTE_STATE_SOD_ENABLED` / `OMS_FIX_ROUTE_STATE_SOD_CRON` | Optional start-of-day: set all `fix_route_state.send_enabled=true` on cron (`FixRouteStateSodScheduler`). |
| `OMS_FIX_SOCKET_USE_SSL` / `OMS_FIX_SOCKET_*STORE*` / `OMS_FIX_ENABLED_SSL_PROTOCOLS` | Initiator TLS to broker (QuickFIX `SocketUseSSL`, keystores, `EnabledProtocols`). |

See `application.yaml` (`oms.fix.*`) and `.env.example`. **Broker UAT soak (human checklist):** [fix-broker-uat-soak.md](fix-broker-uat-soak.md).

## Field mapping (v1)

- **Outbound `NewOrderSingle`:** `ClOrdID` = order UUID string; `Symbol` = `FixSymbolMapper.toVenueSymbol(instrumentSymbol)` from **`oms.fix.symbol-map-json`** (identity when unmapped); `Side`; `OrderQty`; `OrdType` LIMIT if `limitPrice` set else MARKET; `Price` when limit; `TimeInForce` from order string (`DAY` default); `TransactTime` UTC.
- **Inbound `ExecutionReport`:** `ClOrdID` → order id (UUID); `ExecID` → `venue_exec_ref`; `TransactTime` → `venueTs` (fallback `Instant.now()`); **Fill / Partial fill:** `LastQty`, `LastPx` (optional → `0`), `LeavesQty`, `CumQty`; **Canceled:** `ExecType=CANCELED`; **Rejected (new order):** `ExecType=Rejected` → `OrderRejected` / `terminal_reason=VENUE_REJECT`.
- **Inbound `OrderCancelReject` (9):** `OrigClOrdID` (else `ClOrdID`) → order id; `venue_exec_ref` = `ocr-{OrderID}-{CxlRejReason}`; same **`OrderRejected`** path as ER reject.

## Runbook notes

- With **`OMS_ROUTING_BACKEND=fix`** and **`OMS_FIX_AUTO_START=false`**, the app loads FIX beans but does **not** open a TCP session — useful for integration tests and staged deploys.
- Point `OMS_FIX_*` at a paper/test acceptor before production; keep `UseDataDictionary` aligned with the counterparty.
