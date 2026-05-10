# FX backend slice (M3) — execution checklist

This is the **engineering execution index** for the bank-side FX stack described in [fx-architecture-slice8.md](fx-architecture-slice8.md) and master plan §11.5. It does **not** replace that design; it orders **verify** steps so OMS + Ledger + marketdata can be exercised before any **functional** FX desk or Beard FX surveillance UI.

## Verification log (stack runs)

| Date | Track | Evidence (link / commit / dashboard) | Sign-off |
|------|-------|----------------------------------------|----------|
| *Pending* | Quote / nostro / legs / hedge / EOD | Fill when each track completes in non-prod or UAT | |

## Preconditions

- Design sign-off still tracked in product/treasury; code may proceed on **flagged** paths.
- **`GET /internal/v1/fx/health`** reflects **`OMS_FX_MODULE_ENABLED`** (`not_enabled` vs `module_enabled_pending_impl`) — use it as a **deploy smoke** until real modules register here.

## Ordered build / verify tracks

1. **Quote ingress (design §1)**  
   - Marketdata (or dedicated FX price service) publishes versioned topics.  
   - **Verify:** contract tests or MP subscriber sees stable topic naming + schema; OMS or risk service can consume a **stub** quote for integration before production PB.

2. **Nostro inventory (design §2)**  
   - Ledger remains source of truth for FX cash/nostro; OMS reads **snapshots** for limits (no second book).  
   - **Verify:** snapshot HTTP or DB read matches Ledger reconciliation for a test currency pair.

3. **Atomic multi-leg group (design §3 core)**  
   - Customer leg + hedge + nostro movements post in one business transaction where the product demands atomicity (exact leg model from slice8 / finance).  
   - **Verify:** integration test or UAT script: one customer FX flow leaves OMS `orders` / `executions` + Ledger postings consistent under forced failure mid-group (rollback path).

4. **Netting window + hedge routing (design §3–4)**  
   - Internalizer vs external LP routing behind config; limits on open FX delta.  
   - **Verify:** metrics for hedge triggers, pause/resume, per-pair kill switch hooks (master plan §11.5.8) once exposed on internal HTTP.

5. **EOD flatten (design §4)**  
   - Scheduled workflow with same outbox discipline as securities settlement.  
   - **Verify:** dry-run in non-prod with Ledger posting disabled then enabled.

## Exit for “FX UI meaningful” (UI plan alignment)

At least **one** end-to-end path (e.g. internal hedge order or customer FX with quote validity) is observable in OMS + Ledger with metrics; desk routes then consume **read** internal APIs (and optional control endpoints) **without** widening browser access to `X-OMS-Internal-Key` (BFF proxy only).

## Related docs

- [fx-architecture-slice8.md](fx-architecture-slice8.md) — target modules.  
- [plans/oms-ui-implementation-plan.md](../../system-documentation/plans/oms-ui-implementation-plan.md) — desk vs FX UI gates.  
- [plans/oms-realignment-2026-05-07.md](../../system-documentation/plans/oms-realignment-2026-05-07.md) — Phase 1 vs Phase 2 boundary.
