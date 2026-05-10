# Mid-day rollout, canary, and rollback (slice 8 / 14.13)

## What ships today

- **Kill switch:** Postgres `oms_runtime_flags.global_halt` read on each control evaluation; **`GET` / `PATCH /internal/v1/runtime-flags/global_halt`** (internal API key) toggles it. Ops Console wires the PATCH (see Ops Console trading ops slice).
- **Simulated routing canary pause:** `oms_runtime_flags.canary_pause_simulated_fills` — when **`true`**, **`SimulatedReturnPathProjectionWorker`** skips draining synthetic fills while **`OMS_ROUTING_BACKEND=simulated`**. **`GET` / `PATCH /internal/v1/runtime-flags/canary_pause_simulated_fills`** (same internal API key) toggles it; body shape matches other boolean flags (`value`, optional `updatedBy`).
- **Broker / FIX stop:** halting control rejects new risk admission; in-flight FIX behaviour remains broker-specific (document in broker UAT runbook).

## Canary pattern (operator runbook)

1. Deploy new OMS revision with autoscaler max surge **1** (or single replica) in a **non-global-halt** window.
2. Watch **`oms_control_jobs_rejected_stale_total`**, **`oms_fix_*`**, and settlement reconciler metrics for a fixed soak window.
3. On regression: set **`global_halt=true`**, drain queues per `docs/fix-out.md`, then roll back the deployment to the prior image; clear halt only after broker confirms clean state.

## Not yet shipped

- Dedicated **feature-flag service** separate from `global_halt` / boolean `oms_runtime_flags` rows (e.g. per-route rollout %) — tracked as Phase 2 unless product promotes it.
- Automated **rollback webhook** from CI — manual rollback remains the contract for Phase 1.
