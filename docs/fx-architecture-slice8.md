# FX architecture (slice 8 / master plan 11.5)

OMS **does not** host the FX desk stack in Phase 1. This document captures the **intended** modules so Slice 8 exit can reference a single design surface before Java services land.

**Execution checklist (build order, verification):** [fx-backend-slice-m3.md](fx-backend-slice-m3.md).

## Target modules

1. **Quote ingress** — PB / vendor stream into **marketdata-platform** (or dedicated FX price service) with versioned topic naming.
2. **Nostro inventory** — Ledger-backed positions for FX balances; OMS reads snapshots for risk, not double-books cash.
3. **FX risk + hedge** — limits on open FX delta, auto-hedge triggers to external LP or internalizer (Phase 2+ product choice).
4. **EOD flatten** — scheduled workflow posting flatten trades to Ledger with the same outbox discipline as securities settlement.

## UI

Beard / desk console **11.5.8** ships only after modules 1–3 expose read APIs; see `plans/oms-ui-implementation-plan.md` U7+.

## Deferral

Full implementation is **Phase 2 engineering** unless product re-prioritises; OMS changes remain limited to **config flags + docs** until finance signs cash-flow diagrams.
