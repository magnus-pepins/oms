# Runbook: Aeron Cluster snapshot (OMS)

Aeron 1.48.0's `ConsensusModule.Context` exposes no `snapshotIntervalNs` setter. Snapshots fire via:

1. **In-process periodic scheduler** (production default) — `OmsClusterSnapshotScheduler` in `oms-cluster-node`, env `OMS_CLUSTER_SNAPSHOT_INTERVAL_MS` (default **300000** = 5 min). Set `0` to disable.
2. **Graceful shutdown** — `OmsClusterSnapshotOnShutdown` when admission is READY (`OMS_CLUSTER_SNAPSHOT_ON_SHUTDOWN`, default `true`).
3. **Operator-driven** — `ClusterTool.snapshot` via `./gradlew clusterSnapshot` (pre-restart, schema bumps, drills).

Until a snapshot exists, cluster cold start replays the full event log; restart MTTR grows with log size. Periodic snapshots bound replay to a short tail after the latest snapshot.

## What "snapshot" means here

`ClusterTool.snapshot(clusterDir, out)` flips the **`SNAPSHOT`** entry in the cluster's `ClusterControl` toggle counter. The leader's consensus module observes the toggle on its main loop, calls `OmsAdmissionClusteredService.onTakeSnapshot(...)`, writes the binary admission state (orderIndex + idempotency keys + senderMsgSeq dedupe map; schema v3 as of slice 3d, see `OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION`) to a fresh recording on the events stream, and commits a snapshot entry to the recording log.

`ClusterTool.snapshot` returns `true` once the **request** is acknowledged. The snapshot itself completes asynchronously on the leader. To verify the snapshot is durable on disk before, e.g., killing a member, poll `ClusterTool.describeLatestConsensusModuleSnapshot(out, clusterDir)` — it returns `true` once the recording log has the snapshot entry.

## Local (Gradle)

```bash
# OMS_AERON_CLUSTER_DIR points at the consensus-module directory of a running cluster member.
# If unset, falls back to <OMS_AERON_DIR_BASE>/consensus-module (default: build/aeron-cluster/consensus-module).
OMS_AERON_CLUSTER_DIR=/path/to/cluster-member/consensus-module ./gradlew clusterSnapshot
```

Exit codes:

- `0` — `ClusterTool.snapshot` accepted the request. Snapshot completes asynchronously.
- `1` — request rejected (cluster not running, wrong dir, leader unreachable past `ClusterTool` timeout).

## What to verify after running

1. **Scrape the cluster-node `/metrics` endpoint** (`http://<node>:${OMS_CLUSTER_NODE_METRICS_PORT:-8089}/metrics`, slice 4b) for:
   - `oms_cluster_snapshot_events_total{outcome="write"}` — incremented every time the leader runs `onTakeSnapshot`.
   - `oms_cluster_snapshot_duration_seconds_count{outcome="write"}` and `..._sum{outcome="write"}` — count and total time spent in `onTakeSnapshot`.
   - `oms_cluster_snapshot_bytes_sum{outcome="write"}` — total bytes written across all snapshots since process start.
   - `oms_cluster_snapshot_age_seconds` (slice 4h) — wall-clock seconds since the leader last successfully wrote a snapshot. Should drop to ~0 after scheduler or `clusterSnapshot` succeeds; climbs at 1 s/s thereafter. Drives snapshot-freshness alerts (`oms/docs/cluster-slo.md`).

   If the metric counts are unchanged after `clusterSnapshot` returned `0`, the leader hasn't actually run the snapshot yet — wait or check the leader's logs.
2. Tail the cluster-node logs for `OmsAdmissionClusteredService` for any errors during snapshot publish (`snapshot publication closed` would surface here).
3. Stop one cluster member and restart it. The next boot's `OmsAdmissionClusteredService.onStart` should log `loaded admission snapshot: orders=N` instead of doing a position-0 replay. `OmsClusterSnapshotAdminToolIT` verifies this round-trip in CI.

## When to run operator snapshot (in addition to periodic scheduler)

- **Pre-restart checklist:** before draining a cluster member, optional extra `clusterSnapshot` if `oms_cluster_snapshot_age_seconds` is high.
- **Pre-deploy of a snapshot-schema bump:** `OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION` is checked on `loadSnapshot`; ADR 0001 §Discipline rejects older schemas (no dual-version compat). Take a fresh snapshot under the new code before mixed versions.
- **Retention:** purge floor uses retained snapshots — ensure at least `AERON_RETENTION_SNAPSHOTS_TO_RETAIN` (default 3) exist before enabling retention ([`aeron-archive-retention-enablement.md`](../../../system-documentation/docs/runbooks/aeron-archive-retention-enablement.md)).

## Where the code lives

- Periodic scheduler: `oms/src/main/java/com/balh/oms/cluster/snapshot/OmsClusterSnapshotScheduler.java`
- Shutdown gate: `oms/src/main/java/com/balh/oms/cluster/snapshot/OmsClusterSnapshotOnShutdown.java`
- Operator entry point: `oms/src/main/java/com/balh/oms/cluster/admin/OmsClusterSnapshotAdminTool.java`.
- Gradle task: `clusterSnapshot` in `oms/build.gradle.kts`.
- IT: `oms/src/test/java/com/balh/oms/cluster/admin/OmsClusterSnapshotAdminToolIT.java`
- Metrics exporter: `oms/src/main/java/com/balh/oms/cluster/admin/OmsClusterNodeMetricsExporter.java` — port `OMS_CLUSTER_NODE_METRICS_PORT` (default `8089`).

## Related

- ADR: `oms/docs/adr/0001-aeron-cluster-substrate.md`.
- Substrate plan: `system-documentation/plans/oms-aeron-cluster-substrate.md` § Phase 4.
- Archiving plan: `system-documentation/plans/aeron-archiving-retention-and-restart-strategy.md`.
- Slice 4b meter ids: `oms.cluster.snapshot.duration` (Timer), `oms.cluster.snapshot.events` (Counter), `oms.cluster.snapshot.bytes` (DistributionSummary); tagged `outcome ∈ {write, load}`.
- Slice 4h / scheduler gauge: `oms.cluster.snapshot.age_seconds` — see `oms/docs/cluster-slo.md` § Snapshot freshness.
