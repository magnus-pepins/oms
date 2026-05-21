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

## Zombie cluster-node after restart (`active Mark file detected`)

**Symptom:** `pm2 list` shows `oms-cluster-node` `online` with a recent PID, but every Spring Boot role (`oms-ingress`, `oms-postgres-projector`, `oms-fix-egress`) is in a crash-loop with one or both of:

- `io.aeron.exceptions.DriverTimeoutException: FATAL - no driver heartbeat detected`
- `OMS cluster client failed to connect within 30000ms`

Trading-desk Treasury page shows `HTTP 502` with the Cloudflare error HTML on every panel that hits `oms-ingress` (8088).

**Diagnosis (one command):**

```bash
tail -30 ~/oms/logs/oms-cluster-node-combined.log | grep -E "(active Mark file|started; awaiting)"
```

If the most recent matching line is `active Mark file detected` (from `org.agrona.MarkFile.mapNewOrExistingMarkFile`, originating in `io.aeron.archive.ArchiveMarkFile` or `io.aeron.cluster.ConsensusModule`) and NOT a later `OMS cluster-node started; awaiting shutdown signal`, the cluster-node `main()` crashed during `ClusteredMediaDriver.launch()`.

**Why the JVM is still alive** (pre-2026-05-21 builds): the Prometheus metrics exporter binds port 8089 BEFORE `ClusteredMediaDriver.launch()` runs. The JDK `HttpServer` it uses starts a non-daemon dispatch thread. When `launch()` throws, `main()` exits with the exception but that dispatch thread keeps the JVM alive — so `pm2` sees a healthy PID and never auto-restarts.

**Why the mark file looked "active":** Aeron's `ArchiveMarkFile` / `ClusterMarkFile` store a heartbeat timestamp; the file is treated as "owned by a live process" until that timestamp is older than the liveness window (~20s for the archive). A fast PM2 restart (`pm2 restart oms-cluster-node`) starts the new JVM before the old timestamp has aged out, so the new JVM throws on startup.

**Recovery (manual):**

```bash
export PATH="$HOME/.nvm/versions/node/v24.14.1/bin:$PATH"
pm2 stop oms-ingress oms-postgres-projector oms-fix-egress
pm2 stop oms-cluster-node
# Wait for all PIDs to die (PM2 reports `stopped`); takes ≤ 30s due to kill_timeout.
# DO NOT delete any files under ~/oms/build/aeron-cluster — letting the mark-file
# timestamp age out is the safe recovery; deletion costs admission history (see
# "Journal wipe" below).
sleep 30
pm2 start oms-cluster-node
# Tail until you see "OmsAdmissionClusteredService role change -> LEADER"
sleep 12
pm2 start oms-postgres-projector
sleep 5
pm2 start oms-fix-egress oms-ingress
```

Verify with `curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8088/internal/v1/fx/nostro/snapshot` — expect 401 (auth required, not 502 or connection refused).

**Why this should not happen again from 2026-05-21 builds:**

- `OmsClusterNodeBootstrap.main()` now wraps `ClusteredMediaDriver.launch()` + downstream launches in a try/catch that closes the metrics exporter and calls `System.exit(1)` on any failure. PM2 sees a real crash and applies `restart_delay` + `max_restarts`.
- `oms-cluster-node` `max_restarts` was bumped from 10 → 30 (≈155s of retries at 5s `restart_delay`), comfortably outliving Aeron's ~20s mark-file liveness window so the retry loop self-recovers without operator action.

If you still see this on a 2026-05-21+ build, capture `~/oms/logs/oms-cluster-node-combined.log` and `~/oms/build/aeron-cluster/archive/archive-*-error.log` before recovery and attach to the post-mortem.

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

3. **Smoke admin paths** (non-destructive): `scripts/oms-smoke-admin-cancel-paths.sh` from `~/oms` after sourcing `~/.oms-bench.env`. Set `OMS_SMOKE_WORKING_ORDER` / `OMS_SMOKE_TERMINAL_ORDER` for full coverage.

4. **Mark order terminal in Postgres** (preferred — audited HTTP; no cluster event):

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
