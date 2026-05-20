# Runbook: OMS Aeron cluster restart and journal-loss recovery

**Audience:** operators on Pop / bench stacks using `~/oms/ecosystem.config.cjs`.

**Related:**

- Snapshot before restart: [`oms-cluster-node-snapshot.md`](oms-cluster-node-snapshot.md)
- Known issues (force-cancel 410, orphaned orders): [`../../../system-documentation/handovers/2026-05-20-oms-known-issues.md`](../../../system-documentation/handovers/2026-05-20-oms-known-issues.md) §1
- ADR: [`../adr/0001-aeron-cluster-substrate.md`](../adr/0001-aeron-cluster-substrate.md)

---

## What the cluster owns vs Postgres

| Store | Owns |
|-------|------|
| **Aeron cluster** (`oms-cluster-node`) | Live admission state: open orders, idempotency keys, cancel/replace semantics. Source of truth for **new** commands. |
| **Postgres projector** (`oms-postgres-projector`) | Mirror: `orders`, `executions`, settlement tables, `domain_event_outbox`, ledger outboxes. What UIs and BFFs read. |

Postgres can show a **WORKING** order while the cluster **no longer has that order in memory**. That happens after a **journal wipe** or a restart that replays an empty/truncated log without restoring admission snapshots for those orders.

`POST /internal/v1/admin/orders/{id}/force-cancel` (deployed 2026-05-20) detects this: **410 Gone** `cluster_forgot_order` means the cluster swallowed the cancel and the operator must fix Postgres (or accept broker drift — see `AdminOrderController` javadoc).

---

## Normal rolling restart (journal preserved)

**Goal:** bounded replay time; no orphaned orders.

1. **Snapshot** (leader must be up):

   ```bash
   cd ~/oms
   OMS_AERON_CLUSTER_DIR=build/aeron-cluster/consensus-module ./gradlew clusterSnapshot
   # Wait for describeLatestConsensusModuleSnapshot / metrics age ~0 — see oms-cluster-node-snapshot.md
   ```

2. **Restart order** (shared Aeron media driver — cluster first, then consumers):

   ```bash
   export PATH="$HOME/.nvm/versions/node/v24.14.1/bin:$PATH"
   pm2 restart oms-cluster-node
   # Wait until cluster-node logs show healthy leader / snapshot load
   pm2 restart oms-postgres-projector
   pm2 restart oms-fix-egress
   pm2 restart oms-ingress
   ```

3. **Verify**

   - `oms-ingress` listens on **8088**; internal key from `~/.oms-bench.env`
   - Pick a recent WORKING order: `POST .../force-cancel` → expect **200** `force_cancel_applied` or **409** if already terminal — **not** 410
   - `pm2 logs oms-cluster-node --lines 50` — look for `loaded admission snapshot: orders=N` on boot (not only position-0 replay)

`kill_timeout: 30000` on cluster roles is intentional (clean MediaDriver shutdown).

---

## Journal wipe / truncate (dev only — causes orphans)

**Typical mistake:** `rm -rf ~/oms/build/aeron-cluster` (or deleting consensus-module / archive dirs) to “fix” Aeron lock files or after a bad deploy. That **deletes admission history**. Postgres rows from before the wipe are **not** automatically cancelled.

### Symptoms

- Trading desk / beard-admin: orders stuck **WORKING**; cancel from UI times out (broker path).
- `POST /internal/v1/admin/orders/{uuid}/force-cancel` → **410** `cluster_forgot_order`.
- `domain_event_outbox` has **no** pending cancel event for that order (cluster never applied cancel).
- Cluster logs: `applyCancelOrder` is a silent no-op when `orderIndex` misses the id (`OmsAdmissionClusteredService`).

### Recovery procedure (Postgres-only hygiene)

Use only when **410** confirms the cluster forgot the order and the order should be terminal locally (demo cleanup / simulator — not a substitute for a real broker cancel on production venues).

1. **Confirm orphan** (example):

   ```bash
   KEY=$(grep OMS_INTERNAL_API_KEY ~/.oms-bench.env | cut -d= -f2- | tr -d '"')
   curl -sS -X POST "http://127.0.0.1:8088/internal/v1/admin/orders/<ORDER_UUID>/force-cancel" \
     -H "X-OMS-Internal-Key: $KEY" -H "Content-Type: application/json" -d '{}'
   # Expect 410 + cluster_forgot_order
   ```

2. **Inspect executions** (leave history if present):

   ```sql
   SELECT id, order_id, exec_type, settlement_status::text
   FROM executions WHERE order_id = '<ORDER_UUID>';
   ```

3. **Mark order terminal in Postgres** (preferred — audited HTTP; no cluster event):

   ```bash
   KEY=$(grep OMS_INTERNAL_API_KEY ~/.oms-bench.env | cut -d= -f2- | tr -d '"')
   curl -sS -X POST \
     "http://127.0.0.1:8088/internal/v1/admin/orders/<ORDER_UUID>/force-mark-cancelled-postgres-only" \
     -H "X-OMS-Internal-Key: $KEY" \
     -H "Content-Type: application/json" \
     -d '{"reason":"journal-loss cleanup 2026-05-20"}'
   # Expect 200 postgres_cancel_applied; 409 if already terminal or version race
   ```

   Raw SQL fallback (same effect, no HTTP audit):

   ```sql
   UPDATE orders
   SET status = 'CANCELLED',
       version = version + 1,
       terminal_at = NOW()
   WHERE id = '<ORDER_UUID>'
     AND status NOT IN ('CANCELLED', 'FILLED', 'REJECTED', 'EXPIRED');
   ```

4. **Reconcile dependent state** if needed:

   - Open **settlement** executions on that order: mark failed or leave as-is per ops policy.
   - **Positions** inflated by a fill without settlement unwind: use settlement manual actions or `markTradeFailed` on the TRADE execution — see [`../settlement.md`](../settlement.md).

5. **Prevent recurrence:** after wipe, take a fresh snapshot once the cluster is healthy again (`clusterSnapshot`), or avoid wiping — fix lock files with ordered `pm2 stop` / `pm2 start` per role.

---

## Rebuild vs replay (decision table)

| Situation | Action |
|-----------|--------|
| Planned deploy, journal intact | Snapshot → rolling `pm2 restart` (above) |
| Stale Aeron mark file / hung JVM | `pm2 stop` cluster roles → wait 30s → `pm2 start` — **do not** delete dirs unless you accept orphan cleanup |
| Corrupt archive / intentional reset | Wipe journal dirs → **run orphan detection** on all non-terminal `orders` → Postgres cleanup for each 410 |
| Schema bump (`SNAPSHOT_SCHEMA_VERSION`) | Follow deploy notes in [`oms-cluster-node-snapshot.md`](oms-cluster-node-snapshot.md) — snapshot under new code before mixed versions |

---

## Demo stack: loopback simulator hazards

Pop runs `oms-fix-loopback-acceptor` (`FixLoopbackAcceptorMain` / `FixRoundTripAcceptorApplication`):

- **MARKET** orders get an immediate synthetic **FILL** with no position pre-check.
- A **SELL** can therefore fill with **no** prior BUY/settled position → settlement auto-step may hit `settling → settled` and mark the execution **failed** (OMS-2 fix, 2026-05-20).

This is expected on the simulator bench, not a production venue bug. For settlement demos, prefer BUY-first flows or use `markTradeFailed` / manual settlement actions on stray SELL fills.

With `OMS_SETTLEMENT_AUTO_STEP_SCHEDULER_ENABLED=true`, unfunded `inv-{accountId}-USD` rows tombstone in `ledger_settlement_outbox` after 10 skippable attempts (OMS-3).

---

## Config reference (bench)

| Env | Role |
|-----|------|
| `OMS_HTTP_INTERNAL_API_KEY` | `X-OMS-Internal-Key` on `/internal/v1/**` |
| `OMS_ADMIN_CANCEL_OBSERVATION_TIMEOUT_MS` | Force-cancel poll window (default 2000) |
| `OMS_AERON_DIR_BASE` | Default `build/aeron-cluster` under `~/oms` |
| `OMS_SETTLEMENT_AUTO_STEP_SCHEDULER_ENABLED` | Demo settlement driver — **off** with real broker files |

---

## Metrics / logs

| Signal | Meaning |
|--------|---------|
| `oms_orders_admin_force_cancel_cluster_forgot_total` | 410 path taken — investigate journal loss |
| `oms_orders_admin_force_cancel_applied_total` | Cluster + projector observed cancel |
| `loaded admission snapshot: orders=N` (cluster-node boot) | Replay shortened by snapshot |
| `auto-step scheduler: poison pill` | Repeated settlement advance failure → execution marked `failed` |
