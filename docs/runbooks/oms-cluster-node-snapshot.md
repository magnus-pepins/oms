# Runbook: Aeron Cluster snapshot (operator-driven)

Phase 4 slice 4a (`system-documentation/plans/oms-aeron-cluster-substrate.md`) of the substrate plan introduces this. Aeron 1.48.0's `ConsensusModule.Context` exposes no `snapshotIntervalNs` setter — auto-snapshots are not built into the framework. Snapshots fire only via two paths:

1. **Operator-driven** through `ClusterTool.snapshot(clusterDir, PrintStream)` — this runbook's path.
2. **Application-driven** from inside the `ClusteredService` via `Cluster.scheduleTimer` + `ClusterControl.ToggleState.SNAPSHOT.toggle(...)` — deferred to slice 4a-2 only if 4e benchmarking shows operator-driven snapshots leave too much log to replay between cron ticks.

Until slice 4a lands in production with a CronJob (Phase 5), the **only** snapshots a running OMS cluster takes are the ones operators trigger. Until that, every cluster-node restart replays the full log from byte 0 — restart MTTR grows linearly with log size. This runbook keeps that bounded.

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
   - `oms_cluster_snapshot_age_seconds` (slice 4h) — wall-clock seconds since the leader last successfully wrote a snapshot. Should drop to ~0 immediately after `clusterSnapshot` succeeds; climbs at 1 s/s thereafter. Drives the `OmsClusterSnapshotStale` / `OmsClusterSnapshotVeryStale` Prometheus alerts (`oms/docs/cluster-slo.md`).

   If the metric counts are unchanged after `clusterSnapshot` returned `0`, the leader hasn't actually run the snapshot yet — wait or check the leader's logs.
2. Tail the cluster-node logs for `OmsAdmissionClusteredService` for any errors during snapshot publish (`snapshot publication closed` would surface here).
3. Stop one cluster member and restart it. The next boot's `OmsAdmissionClusteredService.onStart` should log `loaded admission snapshot: orders=N` instead of doing a position-0 replay. Slice 4a's smoke IT (`OmsClusterSnapshotAdminToolIT`) verifies this round-trip in CI; slice 4b extends it to assert the load also fires `oms.cluster.snapshot.{duration,events,bytes}{outcome="load"}`.

## When to run this in production

- **Phase 5 cron**: a k8s `CronJob` calls `clusterSnapshot` on a schedule; that lands with the rest of Phase 5 deployment + alerts.
- **Pre-restart checklist**: before draining a cluster member for a rolling restart, run `clusterSnapshot` and wait for `describeLatestConsensusModuleSnapshot` to return true. This caps the new replica's recovery time.
- **Pre-deploy of a snapshot-schema bump**: `OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION` is checked on `loadSnapshot`; ADR 0001 §Discipline rejects older schemas (no dual-version compat). The deploy procedure for a schema bump must take a fresh snapshot under the new code, then prune older snapshots before older replicas can attempt a load.

## Where the code lives

- Operator entry point: `oms/src/main/java/com/balh/oms/cluster/admin/OmsClusterSnapshotAdminTool.java`.
- Gradle task: `clusterSnapshot` in `oms/build.gradle.kts`.
- IT: `oms/src/test/java/com/balh/oms/cluster/admin/OmsClusterSnapshotAdminToolIT.java` — boots cluster, takes snapshot, restarts cluster, asserts `loadSnapshot` reloaded the admission state, and (slice 4b) asserts `oms.cluster.snapshot.*{outcome=write|load}` meters all fire across the round-trip.
- Metrics exporter (slice 4b): `oms/src/main/java/com/balh/oms/cluster/admin/OmsClusterNodeMetricsExporter.java`. JDK `HttpServer` + Micrometer `PrometheusMeterRegistry`, listens on `OMS_CLUSTER_NODE_METRICS_PORT` (default `8089`), serves `/metrics` in Prometheus 0.0.4 text format.
- Exporter unit test: `oms/src/test/java/com/balh/oms/cluster/admin/OmsClusterNodeMetricsExporterTest.java`.

## Related

- ADR: `oms/docs/adr/0001-aeron-cluster-substrate.md`.
- Substrate plan: `system-documentation/plans/oms-aeron-cluster-substrate.md` § Phase 4 slice 4a (operator path) and slice 4b (snapshot observability).
- Snapshot schema definition: `OmsAdmissionClusteredService.SNAPSHOT_SCHEMA_VERSION` (currently v3, slice 3d).
- Slice 4b meter ids: `oms.cluster.snapshot.duration` (Timer), `oms.cluster.snapshot.events` (Counter), `oms.cluster.snapshot.bytes` (DistributionSummary, baseUnit=`bytes`); all tagged `outcome ∈ {write, load}`.
- Slice 4h meter id: `oms.cluster.snapshot.age_seconds` (Gauge, baseUnit=`seconds`, untagged) — drives the snapshot-freshness alert. Cross-link: `oms/docs/cluster-slo.md` § Snapshot freshness.
