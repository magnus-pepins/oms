# Control-plane risk checks (slice 2)

These checks run in **`ControlTailer`** after the **stale-queue** guard and **before**
the optional Ledger buying-power gate and the WORKING CAS.

| Check | `RejectCode` | Config | Notes |
|-------|--------------|--------|--------|
| **Global halt** | `RISK_KILL_SWITCH` | Postgres `oms_runtime_flags` row `global_halt=true` | Interim until Ops toggles Redis-backed flags. Ops Console toggles via `GET`/`PATCH /internal/v1/runtime-flags/global_halt` (internal API key). |
| **Instrument allowlist** | `RISK_INVALID_INSTRUMENT` | `OMS_RISK_INSTRUMENT_ALLOWLIST_ENABLED`, `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` (comma-separated, uppercased at compare) | When disabled, all symbols pass. |
| **Tradable universe (v1)** | `RISK_INSTRUMENT_NOT_ALLOWED` | `OMS_RISK_INSTRUMENT_TRADABILITY_CHECK_ENABLED`, `OMS_RISK_TRADABLE_INSTRUMENT_SYMBOLS` | Runs **after** the allowlist when both are on. **v1** is a config list only; a marketdata-backed **`instruments`** cache (§10) is still future work. When enabled with an empty symbol list, no symbol passes (fail-closed). |
| **Fat-finger limit price** | `RISK_FAT_FINGER_PRICE` | `OMS_RISK_FAT_FINGER_MAX_LIMIT_PRICE` | `0` disables. Compares to order `limit_price` when present. |
| **Fat-finger quantity** | `RISK_FAT_FINGER_SIZE` | `OMS_RISK_FAT_FINGER_MAX_ORDER_QUANTITY` | `0` disables. |
| **Notional cap** | `RISK_NOTIONAL_CAP` | `OMS_RISK_MAX_ORDER_NOTIONAL` | `0` disables. Uses `quantity × limit_price` when both present. |

Every **PASS** or **REJECT** outcome records one row in **`control_decisions`** (Flyway `V5`).

Stale control events still reject with **`RISK_STALE_QUEUE`** and increment **`oms_control_jobs_rejected_stale_total`**.

See [configuration.md](configuration.md) for env keys and [../system-documentation/plans/oms-phase0-interim-decisions.md](../system-documentation/plans/oms-phase0-interim-decisions.md) for interim `OMS_CONTROL_MAX_JOB_AGE_MS`.
