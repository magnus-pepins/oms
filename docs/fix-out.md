# FIX outbound and inbound (slice 4+)

This document holds session config, `NewOrderSingle` / `ExecutionReport` field maps,
environment variables, and runbooks for **QuickFIX/J** wired to `FixRouteDispatcher` and
`ExecutionReportApplier`.

## Current state (slice 4 wiring)

- **Dependencies:** `org.quickfixj:quickfixj-core` + `quickfixj-messages-fix44` (see `build.gradle.kts`).
- **Smoke test:** `FixLogonSmokeTest` — embedded acceptor + initiator logon on a loopback port.
- **Spring IT:** `FixRoutingSpringIntegrationTest` — `oms.routing.backend=fix`, `oms.fix.auto-start=false`, asserts `RouteDispatcher` is `FixRouteDispatcher`.
- **Round-trip IT:** `FixRoundTripSpringIntegrationTest` (profile `fix-roundtrip-it`) — embedded loopback **acceptor** (`FixRoundTripEmbeddedAcceptor`, `SmartLifecycle` phase before initiator) + `oms.fix.auto-start=true`; inserts `WORKING` order, `RouteDispatcher.enqueueWorkingOrder`, awaits **FILLED** and asserts **`oms_fix_nos_sent_total`** / **`oms_fix_inbound_execution_reports_total`** (`disposition=trade_APPLIED`).
- **Stale outbound IT:** `FixOutboundStaleSpringIntegrationTest` — same profile; `oms.fix.max-outbound-job-age-ms` bound; old `accepted_at` → dequeue rejects with **`FIX_OUTBOUND_JOB_EXPIRED`** (no NOS to acceptor; **`oms_fix_outbound_job_expired_total`**).
- **Return-path IT:** `VenueRejectReturnPathIntegrationTest` — `ExecutionReportApplier.applyVenueReject` → `REJECT` execution + **`OrderRejected`**.
- **Outbound:** `FixRouteDispatcher` enqueues `WORKING` order ids into a bounded `BlockingQueue` (capacity `oms.fix.outbound-queue-capacity`). When `oms.fix.auto-start=true`, `FixInitiatorManager` starts a `SocketInitiator` and `FixOutboundDispatchWorker` polls `oms.fix.outbound-poll-interval-ms`, loads the order, builds `NewOrderSingle` (`FixNewOrderSingleBuilder`), and `Session.sendToTarget` on the active session from `FixSessionRegistry`.
- **Inbound:** `OmsFixApplication` routes **`ExecutionReport`** and **`OrderCancelReject`** in `fromApp` → `FixInboundHandler` (`@Transactional`) → `FixExecutionReportMapper` → `ExecutionReportApplier` for **PartialFill/Fill**, **Canceled**, **Rejected** (`ExecType=Rejected` → `OrderRejected` + `executions` **REJECT**), and **OrderCancelReject** (same venue-reject path).
- **Outbound stale jobs:** when `oms.fix.max-outbound-job-age-ms` &gt; `0`, a **WORKING** order older than that at dequeue is **not** sent; `ExecutionReportApplier.applyOutboundJobExpired` CAS to **`REJECTED`** / **`FIX_OUTBOUND_JOB_EXPIRED`** (metric **`oms_fix_outbound_job_expired_total`**).
- **Metrics:** `oms_fix_nos_sent_total` (successful outbound `sendToTarget`); `oms_fix_inbound_execution_reports_total` with tag `disposition` = `trade_*` / `cancel_*` / `venue_reject_*` / `ocr_*` / `ignored`; **`oms_fix_outbound_job_expired_total`**.
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

See `application.yaml` (`oms.fix.*`) and `.env.example`.

## Field mapping (v1)

- **Outbound `NewOrderSingle`:** `ClOrdID` = order UUID string; `Symbol` = `instrumentSymbol`; `Side`; `OrderQty`; `OrdType` LIMIT if `limitPrice` set else MARKET; `Price` when limit; `TimeInForce` from order string (`DAY` default); `TransactTime` UTC.
- **Inbound `ExecutionReport`:** `ClOrdID` → order id (UUID); `ExecID` → `venue_exec_ref`; `TransactTime` → `venueTs` (fallback `Instant.now()`); **Fill / Partial fill:** `LastQty`, `LastPx` (optional → `0`), `LeavesQty`, `CumQty`; **Canceled:** `ExecType=CANCELED`; **Rejected (new order):** `ExecType=Rejected` → `OrderRejected` / `terminal_reason=VENUE_REJECT`.
- **Inbound `OrderCancelReject` (9):** `OrigClOrdID` (else `ClOrdID`) → order id; `venue_exec_ref` = `ocr-{OrderID}-{CxlRejReason}`; same **`OrderRejected`** path as ER reject.

## Runbook notes

- With **`OMS_ROUTING_BACKEND=fix`** and **`OMS_FIX_AUTO_START=false`**, the app loads FIX beans but does **not** open a TCP session — useful for integration tests and staged deploys.
- Point `OMS_FIX_*` at a paper/test acceptor before production; keep `UseDataDictionary` aligned with the counterparty.
