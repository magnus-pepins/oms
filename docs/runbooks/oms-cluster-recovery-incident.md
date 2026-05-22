# Runbook: OMS cluster recovery incident (`orders=0` / 410 after restart)

**Audience:** human operator. **Not** an agent runbook.

**Related:**

- Plan: [`../../../system-documentation/plans/oms-cluster-recovery-and-hardening.md`](../../../system-documentation/plans/oms-cluster-recovery-and-hardening.md)
- Restart: [`oms-cluster-restart.md`](oms-cluster-restart.md)
- Agent rule: [`../../../system-documentation/.cursor/rules/no-oms-mutations.mdc`](../../../system-documentation/.cursor/rules/no-oms-mutations.mdc)

---

## What this runbook is for

After a restart, cluster logs show **`replay validation:`** or **`loaded admission snapshot:`** with **`orders=0`**, while Postgres still has **WORKING** orders. Admin force-cancel returns **410 `cluster_forgot_order`**. The cluster did not restore admission state from snapshot/archive.

There is **no** `seedUsers`-style tool for OMS. Recovery is snapshot/replay, journal-loss orphan cleanup, or engineering escalation — **not** agent automation.

---

## Phase 0 — Read-only triage

### 0.0 Smoke

```bash
bash ~/system-documentation/scripts/smoke/oms-end-to-end.sh
```

| exit | meaning |
|------|---------|
| `10` | ingress unhealthy |
| `12` | snapshot age stale |
| `13` | log shows orders=0 / ANOMALY |
| `20` | unreachable |
| `0` | probes green — may be a different failure |

### 0.1 Confirm symptom

```bash
grep -E 'replay validation:|loaded admission snapshot:' \
  ~/oms/logs/oms-cluster-node-combined.log | tail -10
```

### 0.2 Postgres open orders

```bash
psql "$OMS_JDBC_URL" -c "SELECT status, COUNT(*) FROM orders GROUP BY 1 ORDER BY 1;"
```

Compare WORKING count to cluster log `orders=N`.

### 0.3 Archive / snapshot on disk

```bash
du -sh ~/oms/build/aeron-cluster/archive
ls ~/oms/build/aeron-cluster/archive/archive-*-error.log 2>/dev/null && \
  tail -20 ~/oms/build/aeron-cluster/archive/archive-*-error.log
grep 'loaded admission snapshot' ~/oms/logs/oms-cluster-node-combined.log | tail -5
```

### 0.4 Ledger dependency

Customer settlement paths use ledger. If ledger is unhealthy, fix ledger first:

```bash
bash ~/system-documentation/scripts/smoke/ledger-end-to-end.sh
```

---

## Phase 1 — Operator recovery choices

| Situation | Action |
|-----------|--------|
| Archive intact, snapshot missing | `RUN_SNAPSHOT=1` after cluster healthy; investigate why replay produced 0 — escalate to OMS eng |
| Restart script aborted `orders=0` | **Stop.** Do not loop `pm2 restart`. Capture log byte range since restart; open ticket |
| Journal wiped | [`oms-cluster-restart.md`](oms-cluster-restart.md) § Journal wipe — Postgres-only hygiene per 410 order |
| Transient mark file | `restart-pop-oms-cluster.sh` with 35s stop wait (not bare `pm2 restart`) |

---

## Phase 2 — What agents must not do

- `pm2 restart/stop` on any `oms-*` role
- `rm` under `~/oms/build/aeron-cluster`
- `./gradlew clusterSnapshot` without you naming the command
- `restart-pop-oms-cluster.sh` without you naming the script

You run recovery commands yourself.
