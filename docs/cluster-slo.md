# OMS cluster SLOs and Prometheus alerts

Phase 4 slice 4h closeout (`system-documentation/plans/oms-aeron-cluster-substrate.md`). The metric
inventory shipped in slices 4b–4d gives us four signals we can SLO against; this document is the
source-of-truth for what each signal means, which threshold fires which alert, and what the
on-call response should be. Alert rules live in
`ops-console/docker/config/prometheus/alerts.yml` (group `oms_cluster_alerts`) — the Balh
ecosystem's canonical Prometheus rules file (same place as `eks_gateway_alerts`,
`card_gateway_alerts`, `inference_gateway_alerts`).

These SLOs are **objectives**, not contractual SLAs. They are tuned from slice 4e (`clusterBench`
HdrHistogram p99 < 5 ms on a single-node in-process cluster, Apple Silicon JDK 25; see
`oms/docs/cluster-latency-results.md`) plus operator pragmatism, not from production traffic
yet. Phase 5 (k8s-driven multi-process load) is the first chance to refit them against real
numbers. **Until then**: alert thresholds are deliberately permissive, and on-call should treat
firing alerts as a prompt to investigate rather than as definitive evidence of a defect.

## Targets

| Signal | Window | Warning | Critical | Source metric (Micrometer name) |
| --- | --- | --- | --- | --- |
| Ingress accept p99 | rolling 5 min | > 5 ms for 10 min | > 25 ms for 5 min | `oms.cluster.client.commit_round_trip{command="accept_order",outcome="commit"}` (slice 4c) |
| Apply-execution-report p99 | rolling 5 min | > 5 ms for 10 min | > 25 ms for 5 min | `oms.cluster.client.commit_round_trip{command="apply_execution_report",outcome="commit"}` (slice 4c) |
| Projector lag | sample (per scrape) | > 5 s for 10 min | > 30 s for 5 min | `oms.projector.lag_seconds` (slice 4d) |
| FIX-egress lag | sample (per scrape) | > 5 s for 10 min | > 30 s for 5 min | `oms.fix_egress.lag_seconds` (slice 4d) |
| Venue-egress lag (bytes) | sample (per scrape) | > 4096 for 5 min | > 16384 for 2 min | `oms.venue.egress.lag_bytes` (prediction-market hardening 2026-06-03; aligns with `VenueAdmissionGate` max-lag-bytes) |
| Venue-egress lag (seconds) | sample (per scrape) | > 5 s for 10 min | > 30 s for 5 min | `oms.venue.egress.lag_seconds` on `oms-venue-egress` JVM |
| Venue-egress gRPC failures | rate (5m) | > 0.01/s for 2 min | — | `oms.venue.egress.grpc_failures_total` (tags: `rpc`, `status`) |
| Venue projector lag (seconds) | sample (per scrape) | > 5 s for 10 min | > 30 s for 5 min | `venue.projector.lag_seconds` on `venue-postgres-projector` |
| Snapshot freshness | sample (per scrape) | > 1 h for 30 min | > 4 h for 30 min | `oms.cluster.snapshot.age_seconds` (slice 4h, this slice) |
| Cluster ↔ projector open orders | sample (per scrape) | `oms_cluster_reconcile_in_sync == 0` for 5 min | — | `oms_cluster_reconcile_in_sync`, `oms_drift{kind="open_orders"}` (recovery hardening Phase 3) |
| Reconcile poll freshness | sample (per scrape) | `oms_cluster_reconcile_age_seconds > 180` for 5 min | — | `oms_cluster_reconcile_age_seconds` (30 s default poll) |
| Cluster readiness (HTTP) | probe | `/actuator/oms-cluster-readiness` not READY for 2 min | — | `probe_success{job="oms-ingress-readiness"}` (configure blackbox probe on pop) |
| Cluster-node up | sample (per scrape) | — | `up == 0` for 2 min | scrape-target up state |
| Lag publisher producing data | sample (per scrape) | `lag == -1` for 10 min after boot | — | `oms.projector.lag_seconds`, `oms.fix_egress.lag_seconds`, `oms.venue.egress.lag_bytes` (slice 4d / PM hardening sentinel) |

The Prometheus exporter rewrites Micrometer names: `oms.cluster.client.commit_round_trip` →
`oms_cluster_client_commit_round_trip_seconds_bucket` (Timer), etc. The alert expressions below
use the post-rewrite names.

## Why these numbers

### Ingress accept p99 — 5 ms warning, 25 ms critical

Slice 4e's in-process bench measures p99 ~4 ms on a single-laptop Apple Silicon JVM under
1 000 ops/s synthetic load (zero timeouts/errors across all measured runs). The substrate plan
also takes 5 ms p99 as the "ingress is healthy" target. 5 × the steady p99 (= 25 ms) is the
critical threshold: a 25 ms p99 means a meaningful fraction of orders are taking 5+ × the normal
budget, which is enough to draw a human in. Critical is intended for "page on-call", warning
for "Slack signal — check at your desk". The 10-minute warning window suppresses transient
spikes that are normal at the edge of a cluster snapshot or leadership change.

If the bench numbers from a Linux production runner come in noticeably tighter or looser, this
threshold should be re-tuned in a follow-up slice. The numbers are not made up: the source is
slice 4e + slice 4g; the doc points at them rather than restating.

### Apply-execution-report p99 — same shape

The cluster apply path for `ApplyExecutionReportCommand` runs through the same consensus log
and clustered service as `AcceptOrderCommand`, so the same p99 budget applies. They are split
into separate alerts because a regression in one path (e.g. ER state-machine work blowing up)
should not be hidden by the median of the other.

### Projector lag — 5 s warning, 30 s critical

`OmsPostgresProjector` reads cluster events from the Aeron Archive replay stream and applies
them to Postgres in a single `@Transactional` window. The lag gauge (slice 4d) is wall-clock
seconds since the projector last advanced its cursor (`aeron_projector_cursor.last_applied_at`);
it is updated on every successful apply.

5 s of lag is a comfortable upper bound for a healthy projector under normal load (each apply
is one Postgres TX, sub-millisecond on a healthy DB). 30 s of lag means either the projector
is stuck (long TX, FK contention, dead Postgres connection) or the cluster is producing events
faster than the projector can apply — both are actionable. The 5-minute critical window is
short because a 30 s projector lag can compound into FIX egress lag and downstream BFF lag, so
on-call should react quickly.

### FIX-egress lag — 5 s warning, 30 s critical

`OmsFixEgressService` reads cluster `OrderAdmittedEvent`s from the Archive replay, builds and
sends `NewOrderSingle` via QuickFIX, and advances `oms_fix_egress_cursor.last_applied_at`. The
lag gauge has the same shape as the projector. Same threshold rationale — same "lag means
either we're stuck or the cluster is faster than us" interpretation. A separate alert from
projector lag because the FIX path's failure modes (broker session disconnects, busted FIX
sequence numbers) are very different from the projector's (Postgres TX issues, schema lock
contention).

### Snapshot freshness — 1 h warning, 4 h critical

There is no auto-snapshot interval in Aeron 1.48.0's `ConsensusModule.Context` — snapshots fire
either via operator toggle (`ClusterTool.snapshot`, slice 4a) or via an application-scheduled
timer (slice 4a-2, deferred). Phase 5 will add a k8s `CronJob` that runs the
`OmsClusterSnapshotAdminTool` on a schedule. Until that lands in production, **no automated
snapshot path exists**, and an OMS cluster that runs for hours / days without an operator-driven
snapshot will replay its full event log on the next cold start; restart MTTR grows linearly.

`oms.cluster.snapshot.age_seconds` (this slice, slice 4h) is the wall-clock seconds since the
leader last successfully wrote a snapshot via `onTakeSnapshot`. The gauge resets on every
successful snapshot write; it is **not** reset by snapshot load (Aeron does not expose the
original write time of a loaded snapshot, and treating "load" as "fresh" would hide the case
where a member booted from a stale snapshot file with no subsequent snapshot cron tick — see
`OmsAdmissionClusteredService.lastSnapshotWriteEpochMs` javadoc).

The 1 h warning / 4 h critical thresholds assume Phase 5 runs the snapshot cron every 30 min;
warning at 1 h = 2 missed cron ticks, critical at 4 h = 8 missed ticks. **If the cron interval
is tuned**, the alert thresholds in `ops-console/docker/config/prometheus/alerts.yml` must be
re-tuned to stay at "2 × interval" / "8 × interval" — the fact-of-life relationship between
snapshot cadence and alert threshold is documented in
`oms/docs/runbooks/oms-cluster-node-snapshot.md`.

### Cluster-node down — page after 2 min

Same shape as every other Balh `*-Down` alert in the canonical
`ops-console/docker/config/prometheus/alerts.yml`. Cluster-node `/metrics` (slice 4b's
`OmsClusterNodeMetricsExporter`, default port 8089) goes silent → `up == 0` → page after 2 min.
The 2-minute window absorbs Prometheus scrape misses without paging on transient blips.

### Lag publisher absent / sentinel — warn after 10 min

`OmsProjectorCursorLagPublisher` and `OmsFixEgressCursorLagPublisher` (slice 4d) report
`-1.0` as a sentinel meaning "no observation yet" — i.e. the publisher's `@Scheduled` poll has
not run, or the cursor row has never been read, or Postgres returned no row for this projector
/ egress identity. On a healthy cold start this is the value for the first 1–5 s after the
publisher boots. Beyond ~10 min it indicates a real fault (publisher bean missing, profile not
active, or Postgres unreachable at boot and never recovered). Warning, not critical: it doesn't
mean OMS is broken, but the lag SLO is unmonitored until this is fixed.

### Cluster ↔ projector reconcile — 5 min drift, 3 min stale poll

`OmsReconciliationService` (recovery hardening Phase 3) polls the cluster Aeron counter
`oms-cluster-open-orders-count` (non-terminal orders in admission state) and compares it to
`SELECT count(*) FROM orders WHERE status IN ('PENDING_NEW','NEW','WORKING','PARTIALLY_FILLED')`
on the projector. Gauges: `oms_cluster_count{kind=open_orders}`,
`oms_projector_count{kind=open_orders}`, `oms_drift{kind=open_orders}`,
`oms_cluster_reconcile_in_sync`, `oms_cluster_reconcile_age_seconds`.

`OmsClusterReconcileDrift` fires when `in_sync == 0` for 5 minutes with a fresh observation
(`age_seconds < 180`). This is the proactive counterpart to per-order admin **410
cluster_forgot_order** — count mismatch before an operator hits a single order path.

Debug surface: `GET /actuator/oms-cluster-reconcile` on ingress (200 when inSync, 503 with
per-entity breakdown otherwise). Smoke: `system-documentation/scripts/smoke/oms-end-to-end.sh`
exit code **11** when reconcile is not inSync.

### Cluster readiness HTTP probe — 2 min

`OmsClusterNotReady` assumes a Prometheus blackbox (or equivalent) probe on
`GET /actuator/oms-cluster-readiness` with `job="oms-ingress-readiness"`. Until that probe is
wired on pop, the alert rule is inert. Ingress already gates cluster-mutating POSTs with
**503 OMS_CLUSTER_NOT_READY** when the Aeron readiness counter is not READY.

## What the alerts do not catch

* **Cluster correctness** — none of these alerts catch a non-deterministic apply path or a
  missed snapshot schema bump. Determinism is asserted by the codec round-trip and replay
  byte-equal harnesses (slice 2b-1, 3c-determinism); snapshot schema mismatch is caught at
  service start (`SNAPSHOT_SCHEMA_VERSION` check, ADR 0001 § Discipline).
* **Postgres health** — the projector lag and FIX-egress lag alerts will fire if Postgres is
  hung, but they are not Postgres-down alerts. Database availability lives in
  `supabase_postgres_alerts` upstream (slice 4h does not duplicate it).
* **Inbound FIX broker session** — broker-side FIX session faults surface in the FIX-egress
  lag alert (no inbound ER → no cursor advance → lag climbs) but not as a dedicated alert.
  A future slice should add `oms_fix_session_logged_in{session_id="..."}` style gauges and a
  per-session up alert.
* **Cluster role / leadership** — Aeron exposes member counters; we do not alert on them yet.
  A leadership flap shows up as a brief commit-round-trip p99 spike during failover. The 10-min
  warning window deliberately does not page on a single failover.
* **Phase 4 absolute thresholds are illustrative.** They were tuned from slice 4e's bench
  numbers on a single-laptop Apple Silicon dev box, NOT from production traffic. Phase 5 is
  the first chance to refit them.

## Reproduce / verify

```bash
# Check alert YAML syntax + recording-rule semantics:
docker run --rm \
  -v $(pwd)/ops-console/docker/config/prometheus:/rules:ro \
  prom/prometheus:latest \
  promtool check rules /rules/alerts.yml
```

The alerts file is loaded by ops-console's Prometheus container on startup; a syntax error
would crash the whole observability stack on next deploy. `promtool check rules` is the
deploy-clean gate.

## Related

* ADR: `oms/docs/adr/0001-aeron-cluster-substrate.md`.
* Substrate plan: `system-documentation/plans/oms-aeron-cluster-substrate.md` § Phase 4.
* Latency / GC results: `oms/docs/cluster-latency-results.md` (slices 4e–4g).
* Snapshot runbook: `oms/docs/runbooks/oms-cluster-node-snapshot.md` (slice 4a, freshness
  threshold cross-link).
* Slice 4b–4d meter ids: `oms.cluster.snapshot.{duration,events,bytes}`,
  `oms.cluster.client.commit_round_trip`, `oms.projector.lag_seconds`,
  `oms.fix_egress.lag_seconds`.
* Slice 4h meter id (this slice): `oms.cluster.snapshot.age_seconds`.
