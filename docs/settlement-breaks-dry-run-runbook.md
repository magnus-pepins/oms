# Settlement breaks dry-run (closeout **W2.5** / **W6.2** evidence)

**Audience:** trading-ops + settlement + engineer shadow  
**Surfaces:** Beard Admin **`/settlement-breaks`** ([`SettlementBreaksPage`](../../beard-admin/src/components/Settlement/SettlementExecutionsExplorer.tsx) — `mode="breaks-workflow"`), **`/settlement-manual-actions`**, OMS internal settlement HTTP  
**OMS docs:** [settlement.md](settlement.md), [settlement-eod-ingest.md](settlement-eod-ingest.md)

This runbook exercises **Phase 1 closeout** [plans/oms-phase1-closeout.md](../../system-documentation/plans/oms-phase1-closeout.md) **W2.5**: *ingest fixture or synthetic break → resolve via Beard UI → optional `MARK_TRADE_FAILED` path → verify state*.

---

## Engineering pre-flight (route smoke — not a substitute for §3)

Before asking trading-ops for calendar time, confirm Beard Admin serves **`/settlement-breaks`** and sibling settlement routes in CI or locally:

```bash
cd beard-admin && npx playwright test e2e/settlement-routes.spec.ts
```

Last engineering run recorded in [plans/oms-phase1-exit-actions.md](../../system-documentation/plans/oms-phase1-exit-actions.md) **§E** (**W2.5** row): **2026-05-10**, **4/4** tests (chromium). This checks **shell / auth gate** only, not the full four-eyes manual-action rehearsal.

---

## Preconditions

- Non-prod OMS + Beard Admin wired with **`OMS_INTERNAL_BASE_URL`**, **`OMS_INTERNAL_API_KEY`**, Beard settlement RBAC (operator can **stage**, admin can **approve**).
- At least one **`TRADE`** execution in a **break-friendly** settlement state for your scenario (`failed`, or a path you intend to drive to `failed` — see §2).
- Browser session: operator + separate **admin** user for four-eyes approve (or two browsers / incognito).

---

## 1. Obtain or create a “break” execution row

Pick **one** path (document which you used in the evidence bundle):

### A. Fixture broker confirms (`import-json`)

Use OMS **`POST /internal/v1/settlement/broker-confirms/import-json`** with a fixture that targets an existing execution, following [settlement-eod-ingest.md](settlement-eod-ingest.md) and your env’s curl examples. Confirm the execution appears in Beard **`/settlement-overview`** with the expected settlement status.

### B. Multipart **`file-import`**

Use **`POST /internal/v1/settlement/file-import`** with the same **`rows`** model; verify **`settlement_file_import_batch`** in OMS (or Beard **`/settlement-recon`**) shows **`applied`** or intentional **`failed`** per drill design.

### C. Drive `failed` via **`mark-failed`** (fastest for UI-only drill)

If product accepts using **`POST /internal/v1/settlement/executions/{id}/mark-failed`** (see [settlement.md](settlement.md)) to land **`failed`** + position unwind rules for the drill account, record the **execution id** you will triage in Beard.

---

## 2. Beard Admin — breaks workflow

1. Open **`/settlement-breaks`**. Confirm the **workflow banner** and default **7-day failed** window query return your target row (adjust **from / to** if needed, then **Query**).
2. **Detail** on the execution — skim **`rawEnvelopeJson`** and optional **operator triage note** (browser-local; use **Copy note** if pasting into a ticket).
3. **Stage manual action**
   - Primary CTA: **`Stage manual action`** → opens **`/settlement-manual-actions?executionId=<id>&actionType=MARK_TRADE_FAILED`** when that preset matches the drill.
   - Otherwise use **Other action types…** and pick the correct preset (e.g. **`ADVANCE_SETTLEMENT_ONE_STEP`**, **`REGISTER_BROKER_CONFIRM`**).
4. Submit staging as **operator**; then **approve** as **admin** on **`/settlement-manual-actions`** (four-eyes).

---

## 3. Verification (record in evidence)

Checklist — tick in your evidence ticket / folder:

| Step | Evidence to capture |
|------|----------------------|
| Execution id + before/after settlement status | Screenshot or SQL / API JSON snippet |
| Manual action id + `actionType` | Screenshot of list row or `GET …/manual-actions/{id}` redacted output |
| Position / unwind (if `MARK_TRADE_FAILED` or mark-failed path) | OMS `positions` / `position_history` or approved UI summary |
| Breaks page used | Optional screenshot showing banner + row |

---

## 4. Attach to Phase 1 evidence pack

After the run, paste the **ticket URL** or **secure folder** link into [plans/oms-phase1-exit-actions.md](../../system-documentation/plans/oms-phase1-exit-actions.md) **§E** (breaks dry-run row) and add a **history row** in [plans/oms-phase1-closeout.md](../../system-documentation/plans/oms-phase1-closeout.md) when **W2.5** is fully signed off.

**Placeholder until executed:** `W2.5-EVIDENCE-LINK` — replace with real URL.

---

## 5. Rollback / safety

- Run only in **non-prod** unless change board approves production rehearsal.
- Do not use production customer accounts; use dedicated UAT accounts and idempotency keys documented in the broker UAT pack ([plans/oms-phase1-exit-actions.md](../../system-documentation/plans/oms-phase1-exit-actions.md) §A).
