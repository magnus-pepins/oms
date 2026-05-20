# Control-plane risk checks (slice 2)

These checks run in **`ControlTailer`** after the **stale-queue** guard and **before**
the optional Ledger buying-power gate and the WORKING CAS.

| Check | `RejectCode` | Config | Notes |
|-------|--------------|--------|--------|
| **Global halt** | `RISK_KILL_SWITCH` | Postgres `oms_runtime_flags` row `global_halt=true` | **Phase 1 source of truth.** Ops Console toggles via `GET`/`PATCH /internal/v1/runtime-flags/global_halt` (internal API key). A Redis-backed duplicate toggle is a **future** ops/product option (see [closeout W3.2 / Q4](../../system-documentation/plans/oms-phase1-closeout.md)), not current OMS code. |
| **Sanctions / PEP re-check (v1)** | `RISK_COMPLIANCE_SANCTIONS` | `OMS_RISK_SANCTIONS_RECHECK_ENABLED`, `OMS_RISK_SANCTIONS_RECHECK_STRICT`, `OMS_SANCTIONS_CACHE_MAX_AGE_S` | Permissive mode refreshes an in-process cache; **strict** rejects all orders until a downstream screening client is integrated. |
| **Order rate (per account)** | `RISK_RATE_LIMIT` | `OMS_RISK_ORDER_MIN_INTERVAL_MS_PER_ACCOUNT` | Wall-clock spacing between control evaluations; `0` disables. |
| **Tick grid** | `RISK_TICK_SIZE_VIOLATION` | `OMS_RISK_TICK_SIZE_CHECK_ENABLED`, `OMS_RISK_TICK_SIZE_INCREMENT` | Requires positive increment and `limit_price`. |
| **STP gate (placeholder)** | `RISK_STP_GATE` | `OMS_RISK_STP_GATE_ENABLED`, `OMS_RISK_STP_GATE_REJECT_ALL` | **Reject-all** is a test knob only; real venue-calendar STP is future work. |
| **Instrument allowlist** | `RISK_INVALID_INSTRUMENT` | `OMS_RISK_INSTRUMENT_ALLOWLIST_ENABLED`, `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` (comma-separated, uppercased at compare) | When disabled, all symbols pass. |
| **Tradable universe (v1)** | `RISK_INSTRUMENT_NOT_ALLOWED` | `OMS_RISK_INSTRUMENT_TRADABILITY_CHECK_ENABLED`, `OMS_RISK_TRADABLE_INSTRUMENT_SYMBOLS` | Runs **after** the allowlist when both are on. **v1** is a config list only; a marketdata-backed **`instruments`** cache (§10) is still future work. When enabled with an empty symbol list, no symbol passes (fail-closed). CSV is parsed into a set only when the configured string changes (in-process coalesce). |
| **Symbol halt list** | `RISK_SYMBOL_HALT` | `OMS_RISK_INSTRUMENT_SYMBOL_HALT_CHECK_ENABLED`, `OMS_RISK_HALTED_INSTRUMENT_SYMBOLS` | Evaluated **after** global halt and **before** allowlist / tradability. |
| **Fat-finger limit price** | `RISK_FAT_FINGER_PRICE` | `OMS_RISK_FAT_FINGER_MAX_LIMIT_PRICE` | `0` disables. Compares to order `limit_price` when present. |
| **Fat-finger quantity** | `RISK_FAT_FINGER_SIZE` | `OMS_RISK_FAT_FINGER_MAX_ORDER_QUANTITY` | `0` disables. |
| **Notional cap** | `RISK_NOTIONAL_CAP` | `OMS_RISK_MAX_ORDER_NOTIONAL` | `0` disables. Uses `quantity × limit_price` when both present. |
| **FIX route send gate** | `RISK_MARKET_SESSION_CLOSED` | `OMS_RISK_FIX_ROUTE_SEND_ENABLED_CHECK_ENABLED` | When **`OMS_ROUTING_BACKEND=fix`**, rejects if **`fix_route_state.send_enabled`** is false for **`OMS_FIX_ROUTE_KEY`** (reserved enum value reused as session/route gate). |
| **Aggregate position (BUY)** | `RISK_CONCENTRATION_LIMIT` | `OMS_RISK_MAX_AGGREGATE_POSITION_QUANTITY_CHECK_ENABLED`, `OMS_RISK_MAX_AGGREGATE_POSITION_QUANTITY` | Compares **`positions.quantity_total`** (default custody from **`OMS_SETTLEMENT_DEFAULT_CUSTODY_ACCOUNT_ID`**) + order quantity for **BUY** only; `0` max disables even when check is on. Stub until finance defines true portfolio limits. |
| **SELL position (available qty)** | `RISK_INSUFFICIENT_POSITION` | `OMS_RISK_SELL_POSITION_CHECK_ENABLED` (default **`true`**) | **SELL** at control: `positions.quantity_total` must be ≥ order quantity (default custody). |
| **BUY buying power (Ledger)** | `RISK_BUYING_POWER` | `oms.ledger.enabled` + balance id on order | Compares **`availableBalance`** to **notional + estimated commission** (`BuyFundsRequirement`, same default schedule as settlement). **BUY** with balance binding but no positive reference/limit price fails at ingress validation and at buying-power (cannot size funds). |
| **BUY inflight hold** | *(Ledger HTTP, not a reject code)* | `oms.ledger.inflight-*` | Async/sync/coalescer paths reserve **`holdAmount`** (notional + fee) via `ledger_inflight_outbox` payload; MARKET BUY requires reference cap in `limitPrice`. |

Every **PASS** or **REJECT** outcome records one row in **`control_decisions`** (Flyway `V5`); rows are queryable via **`GET /internal/v1/control-decisions`** for ops audit tooling. Each successful insert increments **`oms_control_decisions_recorded_total`** (Micrometer; tags **`outcome`** = `PASS` \| `REJECT`, **`reject_code`** = enum name or **`NONE`** when the DB row has `NULL` reject code).

Stale control events still reject with **`RISK_STALE_QUEUE`** and increment **`oms_control_jobs_rejected_stale_total`**.

See [configuration.md](configuration.md) for env keys and [../../system-documentation/plans/oms-phase0-interim-decisions.md](../../system-documentation/plans/oms-phase0-interim-decisions.md) for interim `OMS_CONTROL_MAX_JOB_AGE_MS` and **§5** (`OMS_SANCTIONS_CACHE_MAX_AGE_S` baseline for closeout **W3.5**).

---

## RejectCode emit map (Phase 1 closeout — W3.3, started 2026-05-10)

Canonical enum: `com.balh.oms.domain.RejectCode`. This table is the **engineering truth** for which codes are emitted where; expand it as checks land. “Reserved” = enum + DB value exists but no Java path emits it yet. **Prometheus mirror of audit inserts:** `oms_control_decisions_recorded_total` (same tags as above) increments in **`ControlDecisionsRepository.record`** only—aligned with every **`control_decisions`** row written through the repository.

All **`control_decisions`** rows written in production originate from **`ControlTailer.apply`** (the sole caller of **`controlDecisions.record`**); tests and manual SQL may insert rows without touching this counter.

| `RejectCode` | Emitted where (summary) | Status |
|--------------|-------------------------|--------|
| `RISK_STALE_QUEUE` | `StaleJobGuard` / `ControlTailer` when job age exceeds `OMS_CONTROL_MAX_JOB_AGE_MS` | Active |
| `RISK_DUPLICATE` | *Not emitted:* ingress duplicate idempotency key returns HTTP 200 with existing order (`OrdersController`) | Reserved (enum only) |
| `RISK_KILL_SWITCH` | `ControlRiskEvaluator` when Postgres `oms_runtime_flags.global_halt` | Active |
| `RISK_BUYING_POWER` | `BuyingPowerAdmission` / `ControlTailer` (notional + fee; rejects when funding price missing) | Active |
| `RISK_INSUFFICIENT_POSITION` | SELL position check in `ControlRiskEvaluator` | Active |
| `RISK_INVALID_INSTRUMENT` | Allowlist in `ControlRiskEvaluator` | Active |
| `RISK_INSTRUMENT_NOT_ALLOWED` | Tradability list in `ControlRiskEvaluator` | Active |
| `RISK_FAT_FINGER_PRICE` / `RISK_FAT_FINGER_SIZE` | `ControlRiskEvaluator` | Active |
| `RISK_RATE_LIMIT` | `ControlRiskEvaluator` | Active |
| `RISK_NOTIONAL_CAP` | `ControlRiskEvaluator` | Active |
| `INTERNAL_ERROR` | Control / ingress error paths | Active |
| `VENUE_REJECT` | FIX inbound: `FixInboundClusterSink` translates `ExecutionReport` `ExecType=Rejected` / `OrderCancelReject` into `ApplyExecutionReportCommand`; cluster + projector apply the reject. | Active |
| `FIX_OUTBOUND_JOB_EXPIRED` | Outbound stale dequeue (slice 4) | Active |
| `RISK_SYMBOL_HALT` | Halted symbol list in `ControlRiskEvaluator` | Active |
| `RISK_CONCENTRATION_LIMIT` | Aggregate BUY position check in `ControlRiskEvaluator` | Active |
| `RISK_MARKET_SESSION_CLOSED` | FIX `send_enabled` gate when check enabled | Active |
| `RISK_COMPLIANCE_SANCTIONS` | `SanctionsExecutionGate` | Active |
| `RISK_TICK_SIZE_VIOLATION` | Tick grid check in `ControlRiskEvaluator` | Active |
| `RISK_STP_GATE` | STP gate stub in `ControlRiskEvaluator` | Active |
