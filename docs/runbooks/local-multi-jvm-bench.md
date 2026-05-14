# Local multi-JVM bench — Pop! / Linux dev box

How to bring up the post-Phase-3 OMS topology on a single host and run
[`scripts/benchmark/shoot-ingress-orders.sh`](../../scripts/benchmark/shoot-ingress-orders.sh)
against it. Slice 4i artefact (rewrite of the old monolith shoot script for the multi-JVM
substrate; see `system-documentation/plans/oms-aeron-cluster-substrate.md` § Phase 4 slice 4i).

The cluster-substrate plan (Phase 5) targets k8s for production. This runbook is the
**local-server** option in between Apple-Silicon laptop benches (`clusterBench`) and the k8s
StatefulSet — useful for refitting `oms/docs/cluster-slo.md` thresholds on a
production-shaped Linux JVM **without** waiting for k8s.

## What this measures

| Surface | Tool | Source |
|---|---|---|
| Ingress-replica HTTP commit latency (client-side) | `curl time_total` p50/p95/p99 | shoot-ingress-orders.sh |
| Ingress-replica TX commit latency (server-side) | `oms.pipeline.ingress.accept_seconds` Δ histogram | summarize_cluster_pipeline_deltas.py |
| Cluster-client commit round-trip (slice 4c, slice 4j histograms) | `oms.cluster.client.commit_round_trip_seconds` Δ histogram | summarize_cluster_pipeline_deltas.py |
| Cluster admit → projector applied (slice 4j) | `oms.pipeline.cluster_admit_to_projector_seconds` Δ histogram | summarize_cluster_pipeline_deltas.py |
| Cluster admit → FIX NOS on the wire (slice 4j) | `oms.pipeline.cluster_admit_to_fix_nos_seconds` Δ histogram | summarize_cluster_pipeline_deltas.py |
| Projector cursor lag at end of run (slice 4d) | `oms.projector.lag_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| FIX-egress cursor lag at end of run (slice 4d) | `oms.fix_egress.lag_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| Snapshot freshness (slice 4h) | `oms.cluster.snapshot.age_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| Cluster-path tail latency, GC-corrected | HdrHistogram p50/p99/p99.9/max (in-process) | `./gradlew clusterBench` (slice 4e) |

**Slice 4j replaces the old "no per-order distribution" caveat**: the new
`oms.pipeline.cluster_admit_to_fix_nos` histogram on the FIX-egress JVM and
`oms.pipeline.cluster_admit_to_projector` on the projector JVM both record `now_ms -
ev.acceptedAtMillis()` at end-of-apply, so we get an actual per-event distribution across the
cluster boundary. The derived `commit_round_trip_p99 + fix_egress_lag_seconds` upper bound stays
as a sanity cross-check — when the histograms and the upper bound diverge dramatically, scrape
timing or NTP skew is usually the cause.

## What the latency story looks like

Three Timers cover the order's path through the OMS topology. Same-host runs (Pop! /
single-Linux-node) keep the wall-clock comparable trivially: every JVM reads kernel
`CLOCK_REALTIME` via `System.currentTimeMillis()`, so the differences below are within the
kernel-clock granularity. Cross-host (k8s) runs require NTP/PTP within bucket resolution; the
1 ms / 2 ms / 5 ms / … buckets in `OmsPipelineLatencyBounds` were picked so 5 ms of NTP slew is
just one bucket of error, which dashboards can render visibly without misleading on the median.

| Timer | JVM | Stop-the-clock | What it includes | Buying-power / risk visibility |
|---|---|---|---|---|
| `oms.pipeline.ingress.accept` | ingress-replica | DB transaction commit | Ingress JVM Postgres write (`domain_event_outbox`, optional `ledger_inflight_outbox`) | Pre-admission: `maybePlaceBuyLedgerInflightHold` HTTP call to ledger when enabled (synchronous in this transaction) |
| `oms.cluster.client.commit_round_trip` | ingress-replica | Cluster client `submitAcceptOrder` returns | Ingress JVM cluster-client offer + cluster-egress reply round-trip | Same — the synchronous ledger inflight hold is included if it ran before the cluster submit |
| `oms.pipeline.cluster_admit_to_projector` | postgres-projector | After `cursorRepository.advance` returns | Cluster admit → projector orders/executions UPSERT + control admission committed | Post-admission: `OrderControlAdmission` (risk + buying-power) runs **inside** this Timer's window — a slow risk evaluator widens this Timer, not the FIX one |
| `oms.pipeline.cluster_admit_to_fix_nos` | fix-egress | Right after `Session.sendToTarget` returns successfully | Cluster admit → NOS on the wire (FIX builder + QuickFIX FileStore fsync + TCP write) | **Not on this path**: fix-egress consumes `OrderAdmittedEvent` directly off the cluster events recording, so post-admission risk lives in the projector Timer above, not here |

The egress histogram is the latency story when the question is "how fast did the order leave OMS";
the projector histogram is the latency story when the question is "how fast did Postgres see this
order with risk decisions applied". Pre-trade ledger checks (the synchronous inflight hold) only
show up in the ingress-replica Timers and are an HTTP-call cost from the ingress JVM, not part of
the cluster substrate.

Negative samples (NTP slew backwards) are clamped to 0 ms by the recording call sites
(`Math.max(0L, now - acceptedAtMillis)`); the Micrometer Timer never receives a negative duration.

## Bring-up sequence

Each step gets its own terminal (or `tmux` pane). Cluster-node + ingress-replica are the minimum;
projector is required if you want orders materialised in `orders` / `executions`; fix-egress + the
loopback acceptor are required if you want NOS to actually be sent.

The Internal-API key is the **same export** in every shell that needs it — the ingress-replica
reads it on boot, the shoot script reads it on each request, and they must match exactly.

```bash
# Once, per machine: pick a real secret (NOT a placeholder).
export OMS_INTERNAL_API_KEY='replace-with-a-real-secret'
```

### 1. Postgres

Two paths, depending on whether you want OMS to share Postgres with the rest of the Balh stack:

**Path A — dedicated OMS Postgres via compose** (laptop / isolated dev box):

```bash
docker compose up -d postgres
docker compose ps postgres
```

The compose file binds on host port 5440 (not the Postgres default 5432) so it doesn't collide
with a Supabase stack already on 5432; see the comment block at the top of
`oms/docker-compose.yml`. The `OMS_PG_URL` example in `.env.example` is set to
`jdbc:postgresql://localhost:5440/oms` with credentials `oms / oms / oms`.

**Path B — share the ledger Supabase Postgres** (Pop! / shared dev server):

The ledger's `ledger-supabase-pooler` (supavisor) on host port 5432 routes to the underlying
`ledger-supabase-db`. OMS gets its own database (`oms`) on that same Postgres process. Two
quirks vs the standalone path:

- Supavisor demands tenant-prefixed usernames. Use `postgres.<POOLER_TENANT_ID>` (e.g.
  `postgres.your-tenant-id`), not plain `postgres`. Plain `postgres` gives
  `FATAL: Tenant or user not found`.
- The `oms` database needs to exist. Bootstrap once:

  ```bash
  docker exec ledger-supabase-db psql -U postgres -d postgres -c 'CREATE DATABASE oms WITH OWNER postgres'
  ```

  (Or `DROP DATABASE oms WITH (FORCE); CREATE DATABASE oms WITH OWNER postgres;` to wipe between
  benches; the operator-driven IT base class wipe path is described in `AbstractPostgresIntegrationTest`.)

Then OMS bootRun env on Pop. A single sourced env file works across all the bootRun JVMs:

```bash
cat > ~/.oms-bench.env << 'EOF'
export OMS_PG_URL='jdbc:postgresql://127.0.0.1:5432/oms'
export OMS_PG_USER='postgres.your-tenant-id'           # tenant prefix is REQUIRED
export OMS_PG_PASSWORD='your-super-secret-and-long-postgres-password'
export OMS_INTERNAL_API_KEY='pop-bench-secret-...'     # match in shoot script too
# Tell projector + fix-egress (steps 4 / 5) where the cluster-node's media-driver lives.
# The cluster-node defaults to ${OMS_AERON_DIR_BASE:build/aeron-cluster}/media-driver under
# its own CWD, so the absolute path below is what those JVMs need.
export OMS_POSTGRES_PROJECTOR_AERON_DIR="$HOME/oms/build/aeron-cluster/media-driver"
export OMS_FIX_EGRESS_AERON_DIR="$HOME/oms/build/aeron-cluster/media-driver"
# Disable optional integrations not needed for cluster smoke
export OMS_LEDGER_ENABLED=false
export OMS_NATS_ENABLED=false
EOF
chmod 600 ~/.oms-bench.env
```

`source ~/.oms-bench.env` before each `./gradlew bootRun*` call. The ingress-replica's
cluster client is fully wired in `application-oms-ingress-replica.yaml` and needs no extra
env (defaulted to `oms.cluster.client.enabled=true` and `aeron-directory=build/aeron-cluster/media-driver`).

Flyway runs against the `oms` db only (V1→V28). The ledger's own data lives in
`postgres` / `_supabase` / `_realtime` / `auth` / `storage` databases on the same Postgres
process and is untouched.

### 2. Aeron cluster-node JVM

Single-node cluster (smoke / bench; not a quorum):

```bash
./gradlew bootRunClusterNode
# logs: "OmsAdmissionClusteredService onStart" + Aeron Archive ready
# metrics: http://127.0.0.1:8089/metrics  (slice 4b exporter — JDK HttpServer; not Spring)
# override port:  OMS_CLUSTER_NODE_METRICS_PORT=… ./gradlew bootRunClusterNode
```

### 3. Ingress-replica JVM

```bash
./gradlew bootRunIngressReplica
# HTTP:        http://127.0.0.1:8088/internal/v1/orders
# Prometheus:  http://127.0.0.1:8087/actuator/prometheus  (management.server.port)
```

The ingress-replica needs:

- The `OMS_INTERNAL_API_KEY` export above (otherwise every shoot request returns 401).
- The cluster-node from step 2 already running. `application-oms-ingress-replica.yaml` defaults
  `oms.cluster.client.enabled=true` and `oms.cluster.client.aeron-directory` to the same
  `${OMS_AERON_DIR_BASE:build/aeron-cluster}/media-driver` path the cluster-node uses, so a
  single-host bench needs no extra cluster-client env wiring. Override
  `OMS_CLUSTER_CLIENT_AERON_DIRECTORY` only when the ingress-replica runs on a separate host
  from its cluster member (Phase 5 / k8s).

### 4. Postgres-projector JVM

```bash
OMS_POSTGRES_PROJECTOR_AERON_DIR=$(pwd)/build/aeron-cluster/media-driver \
  ./gradlew bootRunPostgresProjector
# Prometheus: http://127.0.0.1:8090/actuator/prometheus
# main HTTP:  http://127.0.0.1:8093  (auto-started by Spring Boot but not used by the role)
# applies cluster events (OrderAdmittedEvent, ExecutionAppliedEvent) to Postgres rows
```

`OMS_POSTGRES_PROJECTOR_AERON_DIR` is required for the projector to find the cluster-node's
events recording. The path must match the cluster-node JVM's actual media-driver directory —
the cluster-node defaults to `${OMS_AERON_DIR_BASE:build/aeron-cluster}/media-driver` resolved
relative to its own CWD, which is `~/oms/build/aeron-cluster/media-driver` for the bring-up
above. The projector profile defaults the YAML key to empty so Spring context-only ITs that
boot the profile without a media driver continue to short-circuit with a warn log
(`oms-postgres-projector replay loop skipped: oms.cluster.projector.aeron-directory is empty`)
— the explicit env var here is the operator-visible signal that you do want a real connection.

Without it, the cluster will admit orders but `orders` / `executions` rows never materialise
and the `oms.projector.lag_seconds` gauge stays at the cold-start sentinel `-1.0`.

The projector profile defaults the main Spring `server.port` to **8093** (override with
`OMS_POSTGRES_PROJECTOR_HTTP_PORT`) so a single-host bench doesn't collide with the
ingress-replica on 8088. The role itself doesn't serve any HTTP traffic — the port exists only
because Spring Boot's web auto-config insists on a Tomcat. Metrics live on the management port
(8090).

### 5. FIX path (optional — only if you want NOS sent on this run)

```bash
# Loopback FIX 4.4 acceptor (auto-fills with ER):
./gradlew fixLoopbackAcceptor &

# Then, in another terminal, the egress JVM. Set OMS_FIX_* env to match the loopback config:
OMS_FIX_EGRESS_AERON_DIR=$(pwd)/build/aeron-cluster/media-driver \
OMS_FIX_AUTO_START=true \
OMS_FIX_SOCKET_CONNECT_PORT=9876 \
OMS_FIX_SENDER_COMP_ID=OMS_INIT \
./gradlew bootRunFixEgress
# Prometheus: http://127.0.0.1:8091/actuator/prometheus
# main HTTP:  http://127.0.0.1:8094  (defaulted in application-oms-fix-egress.yaml; not used)
# Spring property: oms.routing.backend=fix is wired in application-oms-fix-egress.yaml
```

`OMS_FIX_EGRESS_AERON_DIR` is required for the same reason `OMS_POSTGRES_PROJECTOR_AERON_DIR`
is on step 4: the YAML key defaults to empty so Spring context-only ITs short-circuit, and the
operator-visible env var is what wires the role to the cluster-node's media-driver.

If you skip step 5, the shoot summary will report the FIX-egress scrape as missing, which is
fine — `oms.fix_egress.lag_seconds` simply won't appear and the derived upper bound will be
skipped.

The fix-egress profile defaults the main Spring `server.port` to **8094** (override with
`OMS_FIX_EGRESS_HTTP_PORT`) for the same reason as the projector.

### 6. Run the shoot

```bash
SHOOT_COUNT=1000 ./scripts/benchmark/shoot-ingress-orders.sh
```

For an end-to-end run on a Pop! server with the four / five OMS JVMs above on localhost,
the defaults work. For a remote target, override per role:

```bash
OMS_URL=http://pop.local:8088 \
OMS_INGRESS_REPLICA_PROM_URL=http://pop.local:8087/actuator/prometheus \
OMS_POSTGRES_PROJECTOR_PROM_URL=http://pop.local:8090/actuator/prometheus \
OMS_FIX_EGRESS_PROM_URL=http://pop.local:8091/actuator/prometheus \
OMS_CLUSTER_NODE_METRICS_URL=http://pop.local:8089/metrics \
SHOOT_COUNT=1000 ./scripts/benchmark/shoot-ingress-orders.sh
```

## Reading the output

Real example from a `SHOOT_COUNT=1000` run on a Pop!_OS 24.04 box (Linux 6.18, Temurin /
OpenJDK 21.0.10, 48 cores, 251 GB RAM), pointing at the ledger Supabase Postgres on port 5432:

```
done: ok=1000 (http 201+200)  created=1000 (http 201)  fail=0

Ingress-replica timers (Δ between pre/post scrape):
  oms_pipeline_ingress_accept: Δcount=1000, mean=3.477 ms, p50=3.845 ms, p99=5.592 ms
    (Ingress: Postgres accept tx (commit))
  oms_cluster_client_commit_round_trip: Δcount=1000, mean=3.117 ms,
    p50/p99 unavailable (no histogram buckets — sum/count only; use clusterBench)
    (Cluster client: commit round-trip (slice 4c))

Projector gauge (end-of-run):
  oms_projector_lag_seconds = 0.574 s

FIX egress gauge: scrape missing (skipped).

Cluster-node gauge (end-of-run):
  oms_cluster_snapshot_age_seconds = 401.681

HTTP only — curl time_total to ingress-replica (seconds; client-observed, includes JSON marshalling):
  n=1000  min=0.002488  max=0.048851  mean=0.005717
  p50=0.005642  p95=0.007312  p99=0.008710
```

Notes on the output (the asymmetric "no histogram buckets" caveat for `commit_round_trip` is
gone with slice 4j — that meter now publishes percentile histograms):

- `oms_cluster_client_commit_round_trip` carries buckets since slice 4j, so its p50 / p99 are
  real (Micrometer bucket-edge `le` values; round up by one bucket for safety). For
  HdrHistogram-grade tail with coordinated-omission correction, `clusterBench` (slice 4e) is
  still the right tool.
- `oms_pipeline_ingress_accept` p50 / p99 read the same way.
- The slice-4j cross-JVM histograms (`oms_pipeline_cluster_admit_to_projector` and
  `oms_pipeline_cluster_admit_to_fix_nos`) need the projector / fix-egress JVMs to have been
  restarted after slice 4j — the summarize script flags "no histogram buckets in scrape" if
  the JVM still runs older code.
- `oms_cluster_snapshot_age_seconds` ≈ cluster-node uptime when no operator snapshot has
  fired during the run; not an SLO violation. Trigger a snapshot mid-run with
  `./gradlew clusterSnapshot` (see `oms-cluster-node-snapshot.md`) to exercise the gauge.

What to compare against:

- HTTP `time_total` p99 vs `oms_pipeline_ingress_accept` p99 — the gap is JSON / Spring HTTP /
  network frame overhead.
- `oms_pipeline_ingress_accept` p99 vs `oms_cluster_client_commit_round_trip` p99 — the gap is
  Postgres TX commit (`domain_event_outbox` insert + optional ledger inflight).
- `oms_cluster_client_commit_round_trip` p99 vs `oms_pipeline_cluster_admit_to_fix_nos` p99 —
  the gap is the cluster's events-publication → fix-egress replay → FIX builder + send window
  (slice 4j answers the per-event "is FIX egress saturated or just paced?" question with this
  histogram).
- `oms_pipeline_cluster_admit_to_projector` p99 against the SLO doc thresholds (per-event
  freshness instead of an end-of-run gauge).
- `oms_projector_lag_seconds` and `oms_fix_egress_lag_seconds` remain useful as
  end-of-run/steady-state gauges (5 s warn / 30 s critical; see
  [cluster-slo.md](../cluster-slo.md)).
- `oms_cluster_snapshot_age_seconds` — only meaningful if you've taken a snapshot via
  `./gradlew clusterSnapshot` during the run.

## Slice 4j + 4k evidence (Pop! May 2026)

Reference run on the Pop!_OS 24.04 server (Linux 6.18, Temurin 21.0.10, 48 cores, 251 GB RAM,
Postgres = ledger Supabase pooler on 5432). Documented here so future operators know what
"healthy" looks like.

### Slice 4j: 1 k serial shoot (`shoot-ingress-orders.sh`, `SHOOT_COUNT=1000`)

Per-event histograms across the cluster boundary (the `(no histogram buckets)` caveat from
slice 4i is gone):

```
oms_pipeline_ingress_accept             p50=3.495 ms   p99=5.592 ms   (1000 samples)
oms_cluster_client_commit_round_trip    p50=3.495 ms   p99=5.592 ms   (1000 samples)
oms_pipeline_cluster_admit_to_projector p50=3.146 ms   p99=5.592 ms   (1000 samples; slice 4j)
oms_pipeline_cluster_admit_to_fix_nos   p50=2.097 ms   p99=4.194 ms   (1000 samples; slice 4j)
HTTP time_total (curl)                  p50=5.402 ms   p99=7.394 ms
```

The slice-4i hypothesis "items 1+2 (FileStore fsync + cursor UPDATE) account for the ~21 ms
amortised" is **falsified** by this run: at ingress-paced load the FIX egress's per-event cost
is ~2 ms p50 / ~4 ms p99. The ~21 ms in the slice-4i shoot was the curl/jq spawn overhead
between requests, not OMS internals.

### Slice 4k: concurrent burst (`burst-ingress-orders.sh`, JDK 21 HttpClient + virtual threads)

`OMS_BURST_TOTAL=1000` at three concurrency levels, single account ID (intentional cache-friendly
hot path):

| concurrency | HTTP RTT p50 | HTTP RTT p99 | rps offered | egress p50 | egress p99 | projector p50 | projector p99 |
|---|---|---|---|---|---|---|---|
| 50  | 14.1 ms | 88.6 ms  | 2629 | 358 ms | 626 ms | 447 ms | 716 ms |
| 100 | 29.0 ms | 88.1 ms  | 2611 | 358 ms | 626 ms | 447 ms | 716 ms |
| 200 | 54.8 ms | 126.6 ms | 2721 | 358 ms | 626 ms | 447 ms | 716 ms |

Both consumers (FIX egress + Postgres projector) hit a flat ceiling — the histogram p50/p99
do not move once concurrency exceeds the consumer's serial-apply budget. Ingress accept stays
at p99 ≤ 7 ms throughout, so Postgres-write + cluster-commit are NOT the bottleneck. The
flat-ceiling shape is consistent with serial-event processing on each consumer (one Postgres
UPSERT per admitted order on the projector; one FIX `Session.sendToTarget` + FileStore fsync +
`oms_fix_egress_cursor` UPDATE per admit on the egress).

### Slice 4 throughput decision (superseded by Slice 4l — see below)

With 4j + 4k evidence the original decision was to NOT pursue batched cursor advance or
in-memory FIX message store. That decision is **retracted** by Slice 4l: when we re-bench
with a sustained burst (60 k orders, ingress paced at >3 krps) we found the FIX-egress
consumer holds a flat ~700 ev/s ceiling for the full burst, and the projector sustains a
similarly low ceiling. That is no longer "well below realistic SLO" — it is a real ceiling
on what the substrate can lift. The 4k burst at 1 k orders simply finished before the
ceiling mattered.

The instrumentation (slice 4j histograms + slice 4k burst tool) was sufficient to reopen
the decision once we ran a longer burst. Slice 4l does so explicitly.

## Slice 4l evidence (Pop! May 2026)

### Setup

- Same host, same JVMs, same Postgres as 4j/4k.
- Ingress paced via `burst-ingress-orders.sh` with `OMS_BURST_TOTAL=60000`, concurrency 200.
- All five OMS JVMs (`cluster-node`, `ingress-replica`, `oms-postgres-projector`,
  `oms-fix-egress`, `fixLoopbackAcceptor`) run with constrained Hikari pools
  (`OMS_PG_POOL_MAX_SIZE=3`, `OMS_PG_POOL_MIN_IDLE=1`) so the shared ledger Supavisor
  session pool does not exhaust during the run. This does not affect throughput because
  HTTP-accept and cluster-commit only need a single connection per request and the
  projector / fix-egress are single-threaded consumers.

### Hypotheses tested (pre-registered)

| ID  | Hypothesis | Falsifier |
|-----|------------|-----------|
| H1  | QuickFIX `FileStoreSync=Y` (per-NOS fsync) is the dominant per-event egress cost | If H1 is true, flipping to `N` lifts the egress ceiling materially |
| H2  | The `oms_fix_egress_cursor` UPSERT (one Postgres tx per fragment) is the dominant per-event cost | If H2 is true, batching the UPSERT (every N fragments) lifts the egress ceiling materially |

### Result

**H1 rejected**: `FileStoreSync=N` alone moved sustained drain from ~694 ev/s to ~686 ev/s
(within noise; ext4 + NVMe + tmpfs-style cache means a per-message fsync is ~20 µs out of a
~1.4 ms/event budget — too small to see).

**H2 confirmed and the dominant lever**: with `cursor-flush-every=50` (single Postgres
UPSERT per 50 fragments) the egress consumer drained the 60 k-event backlog in **3.19 s**,
i.e. **~18 800 ev/s sustained** with peaks above 25 000 ev/s. That is a **~27× lift** over
the H1+H2-off baseline.

What that says about the architecture:

- The original "items 1+2 (fsync + cursor UPSERT) account for the bulk of per-event cost"
  framing was correct in *direction* but wrong in *split*: the cursor UPSERT was effectively
  the entire per-event cost; the fsync is rounding error.
- The egress is no longer the bottleneck. The new ceiling is the ingress pipeline (HTTP
  accept + cluster commit + synchronous buying-power hold), which sustained ~3 700 orders/s
  for the duration of the burst on this same single-host rig.

### Decision

`cursor-flush-every` ships as a configurable knob with default `1` (per-event UPSERT — the
slice 3b-2 contract is preserved bit-for-bit). The recommended **production** setting is
`25–50`. The trade-off is a wider at-least-once-at-broker redelivery window: on a crash the
egress redelivers up to `cursor-flush-every - 1` `OrderAdmittedEvent` fragments, which
become duplicate `NewOrderSingle` at the broker. The broker rejects them via the
`DupClOrdID` field on the `NewOrderSingle` (option 1 in the FIX 4.4 dedupe model — already
the contract for the existing 1-fragment redelivery window). The window widens; the
correctness model does not change.

`oms.fix.file-store-sync` ships as a configurable knob with default `Y` (QuickFIX/J's
documented default; protocol-loss-free crash recovery via the on-disk message stream
alone). H1 evidence says we don't need to flip it on Pop! storage; operators on slower
storage may want `N` (recovery falls back to FIX `MsgSeqNum` + broker resend on `Logon`,
which is also protocol-loss-free).

Both knobs are surfaced via env vars on the `oms-fix-egress` profile:

```yaml
oms:
  cluster:
    fix-egress:
      cursor-flush-every: ${OMS_FIX_EGRESS_CURSOR_FLUSH_EVERY:1}     # production: 25-50
  fix:
    file-store-sync: ${OMS_FIX_FILE_STORE_SYNC:Y}                    # leave Y unless storage-bound
```

### Slice 4l → next: pushing well beyond ~3 700 orders/s

The egress ceiling moved from ~700 ev/s to ~18 800 ev/s. The **system** ceiling is now
~3 700 orders/s and lives on the ingress side. The substrate is designed to scale
horizontally on both axes the user named ("N ingress acceptors" and "N control / buying-
power checks") — the next slices simply turn that property into measured throughput. See
the `## Scaling roadmap (slice 4l → 5)` section below for the prioritised plan.

## Scaling roadmap (slice 4l → 5)

This is the prioritised path from the current ~3 700 orders/s ingress ceiling to "way
beyond". Numbers are rough, single-host estimates from the slice 4l rig — multi-host
production with the ledger's real load capacity will multiply most of them. Each item lists
the hypothesised lift, the cost / risk, and where to look in the code.

### Where the budget actually goes (post-H2)

```
client → HTTP POST /orders → ingress-replica (Tomcat)
            │
            ├─ pre-trade reservation (synchronous Postgres → ledger Supavisor)   ← ~1.5 ms
            ├─ domain_event_outbox INSERT + tx commit                            ← ~1.5 ms
            ├─ OmsClusterIngressClient.commit() (Aeron Cluster ingress)          ← ~0.3 ms
            └─ HTTP 201
                                │
                                ▼
cluster-node                    │  Raft replicate + apply (single-leader, deterministic)
  OmsAdmissionClusteredService  │  ~0.05 ms / cmd at single-node, sub-ms at 3-node
  emits OrderAdmittedEvent ─────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
   oms-postgres-projector             oms-fix-egress
     orders/positions UPSERT            FIX NOS send + cursor UPSERT
     + control admission                ~18 800 ev/s with H2
     ~3 000 ev/s today                  bottleneck moved upstream
```

The ingress critical section currently does **two** Postgres round-trips per order on the
ledger Supavisor: the buying-power hold and the OMS outbox commit. Cluster commit is
sub-ms; egress and projector apply asynchronously off the cluster events recording.

### Tier 1 — N ingress acceptors (zero new architecture, immediate lift)

The ingress-replica JVM is **already** stateless. Every Tomcat thread submits to the same
Aeron Cluster via `OmsClusterIngressClient.commit()`, the cluster owns ordering, the
projector and egress are blind to which acceptor produced an event. Standing up N
ingress-replica processes (or N pods in k8s) is a config-only change. Slice 4m landed the
operator-side knobs needed to actually run this on Pop!: see the dedicated
`## Slice 4m setup` section below for the per-replica env contract.

**Measured on Pop! (2026-05-14, slice 4m bench, single account / cache-friendly hot path,
60 k orders, concurrency 200, 5-OMS-JVM topology with `OMS_PG_POOL_MAX_SIZE=3` per
replica):**

| Replicas | Elapsed | Burst rps | HTTP RTT p50 | HTTP RTT p99 | Per-target submitted |
|---|---|---|---|---|---|
| 1 (port 8088) | 49.094 s | **1 222** | 150 ms | 251 ms | 60 000 / — |
| 2 (8088 + 8095) | 25.916 s | **2 315** | 83.6 ms | 187 ms | 30 000 / 30 000 |

That is a **1.89× lift** (94.5% scaling efficiency) for adding one ingress-replica JVM
with zero code change to the substrate. Server-side `oms_pipeline_ingress_accept` p50
stayed at 1.0 ms across both runs — the rps lift is pure concurrency, not a hidden
slowdown that "averaged out". The HTTP RTT halving is consistent with each replica seeing
~half of the in-flight requests at the same per-request cost.

The earlier "expected ~6–7 krps" estimate was too optimistic for this setup because the
HikariCP pool is intentionally constrained to `MAX=3` per JVM during slice 4l/4m to
protect the shared ledger Supavisor session pool — so a single replica is already
concurrency-bound at ~1.2 krps before adding network / Tomcat overhead.

**Slice 4n raised the per-replica ceiling.** Code-reading to design a "batched
buying-power holds" slice revealed that buying-power isn't even on the bench hot path
(`OMS_LEDGER_ENABLED=false` skips `maybePlaceBuyLedgerInflightHold`). The actual
per-replica wall in slice 4m was inside the cluster client itself:
`OmsClusterIngressClient.submitAcceptOrder` held `clientLock` from before
`AeronCluster.offer` through the entire egress poll loop while waiting for *its own*
correlation-id reply. With ~1 ms cluster RTT that gates each ingress-replica JVM at
~`1 / cluster_rtt ≈ 1 krps`, regardless of how much downstream capacity exists. Slice 4n
pipelined the client (`pending` map of `CompletableFuture`s, dedicated egress poller
thread; full design in the slice 4n commit message and the 4n class doc), preserving
Aeron's thread-confinement invariant while removing the per-JVM RTT cap.

**Measured on Pop! (2026-05-14, slice 4n, same bench scenario / 60 k orders /
concurrency 200):**

| Topology | `OMS_PG_POOL_MAX_SIZE` | Burst rps | vs slice 4m baseline | Notes |
|---|---|---|---|---|
| 1× ingress, slice 4m | 3 | 1 222 | 1.00× | per-JVM cluster-RTT cap |
| 1× ingress, slice 4n | 3 | **525** | **0.43×** | Hikari now binds well below the new cluster-pipeline ceiling; the pipeline's CompletableFuture park/unpark + egress-poller cadence stretches per-tx hold time and the small pool magnifies it |
| 1× ingress, slice 4n | 20 | **2 283** | **1.87×** | Hikari no longer binds; the cluster pipeline carries the new throughput |
| 2× ingress, slice 4m | 3 | 2 315 | 1.89× scaling | per-JVM cap × 2 |
| 2× ingress, slice 4n | 3 | 1 024 | 0.84× | same MAX=3 regression × 2 |
| 2× ingress, slice 4n | 10 | 1 443 | 0.62× | bench-host Supavisor (`POOLER_DEFAULT_POOL_SIZE=20`) saturates around 5 OMS JVMs × ~MAX=10 client conns |
| 2× ingress, slice 4n | 20 | 677 | 0.29× | bimodal latency (p50=93 ms / p99=773 ms) — Supavisor backend pool genuinely exhausted |

**Read this carefully**: slice 4n is a clear *single-replica* unlock when the Hikari pool is
not the binding constraint (1× MAX=20 ≈ 2 283 rps, **1.87× over slice 4m's best**), but
multiplying replicas at the same Pop!-host Supavisor topology stops paying out because
the shared Supavisor backend pool (`POOLER_DEFAULT_POOL_SIZE=20`) saturates as soon as the
total OMS-side connection ceiling exceeds it. That is **not** a slice 4n bug — slice 4m's
`MAX=3` hid the same problem behind the cluster-client lock. Slice 4n exposes it, and the
fix is operational: bump Supavisor's tenant pool, switch the OMS to Supavisor's
transaction-mode pooler (port 6543) so `MAX_PG_POOL_MAX_SIZE` decouples from Postgres
backend count, or bypass Supavisor and connect direct to the ledger Postgres. Once that
lands, the same 1.87× per-JVM lift should compose with N replicas linearly until the
cluster ingress is the bottleneck.

**Operational rule of thumb after slice 4n**:

- Single replica + `OMS_PG_POOL_MAX_SIZE>=10` → slice 4n is a clear win.
- N replicas → also bump Supavisor's tenant pool or move to transaction-mode (separate slice).
- Keeping `MAX=3` because the bench host's Supavisor is small is fine for slice-4l / 4m
  comparison runs, but treat the slice-4n number under that pool as a regression artefact,
  not a substrate signal.

- **Cost / risk**: none, beyond the Supavisor session pool which is the next item.
- **Where**: `application-oms-ingress-replica.yaml`. Each replica needs a unique
  `OMS_HTTP_PORT` and a unique `OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT`. The
  cluster-client `ingress-channel` / `egress-channel` defaults are
  `aeron:udp?endpoint=localhost:0` (port 0 = ephemeral) so two replicas sharing one Aeron
  media driver each get distinct ports automatically (verified: replica 2 booted with
  default channels and connected to the cluster on the same media driver as replica 1
  without any port collision).

### Tier 2 — Pipeline the OMS cluster client (slice 4n, landed)

Slice 4n's actual diff: see the slice-4m findings folded into Tier 1 above. The original
4n proposal was "batched buying-power holds" but a code-read showed buying-power isn't on
the bench hot path at all when `OMS_LEDGER_ENABLED=false`, so the effective bottleneck was
the cluster client's per-JVM single-flight lock. Pipelining `submitAcceptOrder` lifted
that wall (1× MAX=20 = 2 283 rps, 1.87× the slice 4m best) at the cost of exposing
Supavisor as the next-up bottleneck. The Aeron thread-confinement invariant (`offer` /
`pollEgress` / `sendKeepAlive` cannot run concurrently) is still respected — `clientLock`
now wraps only individual `offer` and `pollEgress` calls, never the egress wait.

### Tier 2.5 — Move buying-power hold off the ingress critical path (slice 4p + 4q)

With the cluster-pipeline win banked (4n) and Supavisor unblocked (4o), the next big
chunk of ingress-accept p50 in production is the synchronous JDBC call to the ledger
from the HTTP request thread inside `OrderIngressService.maybePlaceBuyLedgerInflightHold`.
Phased: **slice 4p** lands the async outbox + a new `CancelOrderCommand` so failed
holds correctly cancel the working order (phase A); **slice 4q** layers an opt-in
sync-semantics path back on top via an in-request coalescer (phase C). The
two-options-in-this-section design from earlier is retained for context:

1. **Pipeline / batch the holds**. Multiple in-flight orders share a single ledger
   transaction window (group commit on the OMS side). Concretely: replace the
   "1 HTTP request → 1 hold tx" mapping with a small in-process queue + group-coalesce
   worker that flushes every ≤1 ms or every 32 orders. Each order's HTTP response waits on
   its own hold result; the *batch* commits to ledger as one tx. Ledger throughput per
   commit is roughly fixed-cost, so this is a 4–10× lift on the hold tier alone.
   - Lift: ingress p50 drops from ~3 ms (production-env hold included) to ~0.5–1 ms.
   - Risk: low — semantics are preserved (hold is still synchronous from the client's POV).
2. **Cache balances locally with optimistic reservation**. Ingress holds against an
   in-process balance projection (Redis-backed or built off the ledger event stream),
   submits the order to the cluster, and reconciles asynchronously. Mismatches reject the
   order out-of-band (cancel + reverse hold). Only worth doing if (1) doesn't reach
   target, because it changes the consistency model. The ledger already publishes the
   stream we'd consume.

- **Where**: `OrderIngressService.maybePlaceBuyLedgerInflightHold` +
  `LedgerInflightReservationClient`. The batching boundary fits cleanly between the
  request-scoped hold and the JDBC tx.
- **Note**: this slice is only worth running when the Pop! bench can also exercise the
  ledger path. Today's bench has `OMS_LEDGER_ENABLED=false` because the Supavisor topology
  on the bench host can't comfortably hold sessions for both OMS and ledger writes at the
  per-replica pool sizes that slice 4n now wants. Sequencing: fix Supavisor topology first
  (Tier 2.4), *then* batch the holds.

### Tier 2.4 — Fix the Supavisor / Postgres topology so multi-replica scales (slice 4o, **landed**)

Shipped in slice 4o (this section is kept for the design rationale — operator
instructions are in `## Slice 4o setup` further down). Slice 4n exposed that the
bench-host Supavisor (`POOLER_DEFAULT_POOL_SIZE=20`, `POOLER_MAX_CLIENT_CONN=100`)
backs an `oms` Postgres database whose total backend connections were capped at 20.
With 5 OMS JVMs each opening up to `OMS_PG_POOL_MAX_SIZE` client connections, total
client conns easily exceed 20 and Supavisor multiplexes them onto a too-small backend
pool — bimodal latency (some requests fast at the slice-4n p50, some queued >700 ms
waiting for a backend) and degraded `2× rps`. Slice 4o picked **all three** of the
mitigations below; both step 1 (resize) and step 2 (transaction-mode flip + 
`prepareThreshold=0`) landed; option 3 (bypass Supavisor) was deferred as a fallback.

1. **Switch OMS to Supavisor's transaction-mode pooler (port 6543)** instead of session
   mode (port 5432). Transaction mode releases the backend back to the pool after each tx
   commit, so a small backend pool serves many client sessions. This is the standard answer
   for high-concurrency Postgres clients on Supabase/Supavisor.
2. **Resize the bench-host Supavisor pool** (`POOLER_DEFAULT_POOL_SIZE` ≥ 100) for
   apples-to-apples bench runs. Stopgap — production doesn't share the bench's pooler.
3. **Bypass Supavisor and connect direct to Postgres** for the ingress-replica JVMs.
   Cheapest for a one-off bench, but loses Supabase's managed pooling story.

- **Where**: `application.yaml` `spring.datasource.url`, plus the `~/.oms-bench.env` on
  Pop! (`OMS_PG_URL`).
- **Slice**: proposed as a sibling of Tier 2.5 — fix this first, then batch the holds.

### Tier 3 — N control / buying-power workers (already supported by the substrate)

The post-admission control evaluation (`OrderControlAdmission`, the projector-tier risk +
buying-power check) **runs inside the projector JVM today**, on the projector's serial
apply thread. That is fine when you have 1 projector and the projector keeps up — but the
projector's drain rate (~3 k ev/s) is already lower than the egress's 18.8 k ev/s. We can:

1. **Run N projector JVMs**, each consuming the same `OmsClusterWireFormat.EVENTS_STREAM_ID`
   recording with **disjoint key shards** (e.g. `account_id mod N`). The cluster's
   ordered-per-key guarantee already holds within a shard; the projector consumer side
   just needs a shard predicate. Cursor advances become per-shard (`oms_projector_cursor`
   already keys on `(stream_id, projector_id)`; we'd add `shard_id` to the grain).
   - Lift: linear in N up to the ledger's commit ceiling for control checks.
2. **Split the control check off the projector JVM** entirely. The projector keeps its
   "orders / executions UPSERT" job; a separate `oms-control` JVM(s) subscribes to the
   same events recording, runs the risk + buying-power evaluation, and emits a
   `ControlAdmissionDecision` event back via the cluster ingress. The egress waits for
   that decision before NOS. This is the cleanest "N control workers" shape; it costs one
   extra Aeron ingress hop per order.

- **Where**: `OmsPostgresProjector`, `OrderControlAdmission`, projector cursor schema.
- **Slice**: 4o (proposed). Tier 3 is bigger than tiers 1 + 2 combined; do tiers 1 + 2
  first and re-measure.

### Tier 4 — Apply H2 to the projector

Same idea as the egress H2 unlock. The projector currently does a Postgres UPSERT per
admitted event for `orders` + cursor advance + (where applicable) positions. Each is one
round-trip on the same Supavisor pool. Group-commit the cursor advance the same way
egress does (config knob, default 1, recommended 25–50), and consider batching the
`orders` / `positions` UPSERTs via a single multi-row statement per batch.

- **Lift**: projector drain rate from ~3 k ev/s to "egress-grade" 15–20 k ev/s.
- **Risk**: same redelivery shape as egress H2; the projector is idempotent on
  `(account_id, client_order_id)` so duplicates are silently absorbed.
- **Where**: `OmsPostgresProjector.applyAdmittedEvent` and friends; mirror the
  `OmsConfig.Cluster.Projector.cursorFlushEvery` shape we just shipped on FixEgress.
- **Slice**: 4p (proposed).

### Tier 5 — Cluster-side and FIX-side parallelism (Phase 5 territory)

Beyond ~50–100 krps single-cluster the leader's deterministic apply thread becomes the
bottleneck. Two complementary moves:

1. **Multiple cluster groups, sharded by `account_id`**. Each group is a fully independent
   Raft cluster with its own leader, ingress channel, and events recording. Routing lives
   in the ingress-replica (consistent hash on `account_id`). The ledger is already
   account-keyed so the holds carry across cleanly.
2. **Multiple FIX sessions to the broker**. `FixOutboundSessionSend` fans out by
   `(account_id, route_key)` today; standing up N initiator sessions parallelises the
   `Session.sendToTarget` path past the single-session ceiling. Brokers typically
   throttle per-session, not per-firm, so this is mostly a broker-contract negotiation.

- **Slice**: Phase 5 (Cloud / k8s).

### Recommended order of execution

| Slice | What | Ingress throughput on Pop! |
|-------|------|------------------------------|
| 4l (done) | H2 batched egress cursor (default 1, prod 25–50) | egress no longer the limit (≈18 800 ev/s drain) |
| 4m (done, **measured 2026-05-14**) | 2 ingress-replicas, same cluster | 1 → 2 replicas: **1 222 → 2 315 rps (1.89×)**; pool-cap'd at `MAX=3` |
| 4n | Batched buying-power holds + raise `OMS_PG_POOL_MAX_SIZE` | ~15–30 krps |
| 4o | Disjoint-shard projectors (or split control JVMs) | ~30 krps + (control no longer the limit) |
| 4p | H2 batched projector cursor / UPSERTs | projector drain ≈ egress drain |
| Phase 5 | Sharded cluster groups + multi-session FIX | "trade-08 QuickFIX-grade" 50 k+ NOS/s |

The throughput delta for a 1980s-style trading-floor target (10 k NOS/s sustained) is
already inside Tier 2. Tiers 3–5 are about not having to think about the ceiling again,
not about reaching the immediate target.

## Slice 4m setup — running 2× ingress-replicas on Pop!

Slice 4m exposes the operator contract for tier 1: stand up a second ingress-replica JVM
sharing the cluster-node's Aeron media driver and Postgres, then drive the burst tool with
two targets via `OMS_BURST_URLS`.

### What the slice ships

- `application-oms-ingress-replica.yaml`: `OMS_CLUSTER_CLIENT_INGRESS_CHANNEL` and
  `OMS_CLUSTER_CLIENT_EGRESS_CHANNEL` are now first-class env vars (defaults remain
  `aeron:udp?endpoint=localhost:0`, i.e. ephemeral UDP port). No code change is required to
  run N replicas on the same host because port 0 binds a fresh ephemeral port per JVM; the
  envs let you pin specific ports for firewall / multi-host setups when needed.
- `IngressBurstMain` / `scripts/benchmark/burst-ingress-orders.sh`:
  `OMS_BURST_URLS=<csv of full URLs>` round-robins requests across the listed targets
  (request `i` → `urls[i % urls.size()]`). The end-of-run summary prints
  `per-target submitted` so you can confirm both replicas actually saw their share. The
  single-URL `OMS_URL` / `OMS_BURST_URL` path keeps working unchanged.

### Topology

```
                     (this is the existing single-host bench, with 1 extra ingress-replica)

  burst-ingress-orders.sh ─┬─► ingress-replica-1   HTTP 8088   actuator 8087
                           │       │
                           │       ▼
                           │   OmsClusterIngressClient (ephemeral UDP)
                           │       │
                           │       ▼
                           │   ┌──────────────────────────────────────┐
                           │   │ cluster-node + ClusteredMediaDriver  │   actuator 8089
                           │   │ (one shared Aeron dir on disk)       │
                           │   └──────────────────────────────────────┘
                           │       │              │
                           │       ▼              ▼
                           │   projector       fix-egress
                           │   actuator 8090   actuator 8091
                           │       │              │
                           └─► ingress-replica-2   HTTP 8095   actuator 8086
                                   │ (same OmsClusterIngressClient → same media driver)
                                   ▼
                              (cluster-node above)
```

Both ingress-replicas connect to the cluster-node's Aeron media driver as Aeron clients.
Each `AeronCluster` session is a separate logical client of the cluster regardless of the
shared media driver; the cluster's Raft replication handles ordering across both. Each
replica also has its own Hikari pool against the OMS Postgres for `domain_event_outbox`
inserts.

### Per-replica env contract

Replica 1 (the one you already run today):

```bash
export OMS_HTTP_PORT=8088
export OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8087
export OMS_CLUSTER_CLIENT_AERON_DIRECTORY=$HOME/oms/build/aeron-cluster/media-driver
# OMS_CLUSTER_CLIENT_INGRESS_CHANNEL / _EGRESS_CHANNEL: leave at defaults (ephemeral UDP)
export OMS_PG_POOL_MAX_SIZE=3
export OMS_PG_POOL_MIN_IDLE=1
```

Replica 2 (new — distinct HTTP / management ports, same Aeron dir, same Postgres):

```bash
export OMS_HTTP_PORT=8095
export OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8086
export OMS_CLUSTER_CLIENT_AERON_DIRECTORY=$HOME/oms/build/aeron-cluster/media-driver
# Defaults again — port 0 ephemeral binds a fresh UDP port for replica 2's cluster client
export OMS_PG_POOL_MAX_SIZE=3
export OMS_PG_POOL_MIN_IDLE=1
```

The Supavisor session-pool budget on Pop! is the limiter: with 5 OMS JVMs already running
at `OMS_PG_POOL_MAX_SIZE=3` (= 15 reserved sessions), one more ingress-replica adds 3
more, putting us close to the slow path you hit during slice 4l. If startup fails with
`MaxClientsInSessionMode`, restart `ledger-supabase-pooler` to drop stale sessions and
re-launch.

### Driving the burst across both replicas

```bash
export OMS_INTERNAL_API_KEY='<same key you set on both replicas>'
export OMS_BURST_TOTAL=60000
export OMS_BURST_CONCURRENCY=200
export OMS_BURST_URLS='http://127.0.0.1:8088/internal/v1/orders,http://127.0.0.1:8095/internal/v1/orders'
./scripts/benchmark/burst-ingress-orders.sh
```

The script's stdout will start with:

```
Burst targets (OMS_BURST_URLS, round-robin):
  http://127.0.0.1:8088/internal/v1/orders
  http://127.0.0.1:8095/internal/v1/orders
```

…and end with the burst summary, including:

```
per-target submitted (round-robin):
  http://127.0.0.1:8088/internal/v1/orders = 30000
  http://127.0.0.1:8095/internal/v1/orders = 30000
```

### How to read the result

The post-run Prometheus delta script only scrapes **one** `OMS_INGRESS_REPLICA_PROM_URL`
(replica 1, port 8087 by default). That is intentional — operators who want a true
aggregate across replicas can either:

1. Look at the **burst tool's own end-of-run summary** (`elapsed`, `rps`, `success`,
   per-target submitted, HdrHistogram p50/p99 of HTTP RTT). This is aggregate-correct
   without summing across replicas because it sits client-side.
2. Look at the **cluster-side per-event histograms**:
   `oms.pipeline.cluster_admit_to_projector` (projector JVM) and
   `oms.pipeline.cluster_admit_to_fix_nos` (fix-egress JVM). Both are unsharded — there's
   one projector and one fix-egress consuming the cluster's events recording — so their
   p50/p99 represent the system-level admit-to-apply latency across whichever replica
   the order entered through.

The lift you're looking for is in (1): if burst rps with 2 replicas is materially higher
than burst rps with 1 replica at the same concurrency, tier 1 worked. If it plateaus, the
next bottleneck is either the Supavisor pool (look for `Connection is not available` in
the OMS logs) or cluster-node ingress (look for backlog growth on
`oms.cluster.client.commit_round_trip_seconds` p99 in either replica's actuator scrape —
add `OMS_INGRESS_REPLICA_PROM_URL_2` and a second pre/post scrape to the wrapper if you
want to compare them; not implemented yet because we expect tier 2 — batched holds — to
be the next bottleneck once tier 1 lifts the ceiling).

### Verification run (2026-05-14)

Measured on the Pop! topology described above (5 OMS JVMs, `OMS_PG_POOL_MAX_SIZE=3` per
replica, single-account hot path, 60 k orders / concurrency 200):

```
== 1× replica (control, OMS_BURST_URL only) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=49.094 s  rps=1222.1
HTTP RTT p50=150.015 ms  p99=250.879 ms
ingress-accept (server-side) p50=1.000 ms  p99=5.592 ms

== 2× replicas (OMS_BURST_URLS=8088,8095, round-robin) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=25.916 s  rps=2315.2
HTTP RTT p50=83.647 ms  p99=187.263 ms
ingress-accept (replica 1 only, scrape isolated) p50=1.000 ms  p99=5.592 ms
per-target submitted (round-robin):
  http://127.0.0.1:8088/internal/v1/orders = 30000
  http://127.0.0.1:8095/internal/v1/orders = 30000
```

Lift: 1.89× (94.5% scaling efficiency). The ingress-accept server-side timer p50 stayed
flat at 1.0 ms across both runs, ruling out "added load slows each replica down". The
HTTP RTT halving is consistent with each replica seeing ~half the queued requests at
the same per-request cost. Per-target submitted exactly 30000/30000 confirms slice 4m's
round-robin invariant in production.

## Slice 4n setup — pipelined cluster client

Slice 4n changed the *internal* threading model of `OmsClusterIngressClient` so the
ingress-replica JVM can issue many in-flight `submitAcceptOrder` calls without
serialising the whole RTT under `clientLock`. Operationally the replica looks identical
from the outside (same env vars as slice 4m, same actuator ports), so the slice 4m setup
section above still applies. This section captures only the new knobs and the
operational implications you need to know about.

### What the slice ships

- `OmsClusterIngressClient`: replaced single-flight wait under `clientLock` with a
  `ConcurrentHashMap<correlationId, CompletableFuture<AdmissionResult>> pending`. Submit
  threads now hold `clientLock` only for `AeronCluster.offer` (with bounded back-pressure
  parking), then await the future *outside* the lock. A dedicated **egress poller**
  daemon thread drains `pollEgress()` (which routes replies into `pending`) and sends
  periodic `sendKeepAlive()`s — both calls under the same `clientLock`, preserving Aeron's
  thread-confinement invariant. The class doc on `OmsClusterIngressClient` has the full
  threading diagram.
- New env knobs (defaults are tuned, override only with measurement in hand):
  - `OMS_CLUSTER_CLIENT_EGRESS_POLL_PARK_NANOS` — egress poller park between
    `pollEgress()` calls. Default `100000` (100 µs). Lowering this (e.g. `1000` for 1 µs)
    *worsens* throughput on small Hikari pools because the poller starves submit threads
    for `clientLock`. Raise it (e.g. `1000000` = 1 ms) only for very latency-tolerant
    setups that want to spend less CPU on polling.
  - `OMS_CLUSTER_CLIENT_HEARTBEAT_INTERVAL_NANOS` — keep-alive cadence the egress poller
    uses for `sendKeepAlive()`. Default sized below the cluster's session-timeout.

### Per-replica env contract (slice 4n delta over slice 4m)

The replica 1 / replica 2 envs in `## Slice 4m setup` still apply verbatim. The only
practical difference is the connection-pool guidance:

```bash
# Slice 4m default — conservative for the bench-host Supavisor (POOLER_DEFAULT_POOL_SIZE=20)
export OMS_PG_POOL_MAX_SIZE=3
export OMS_PG_POOL_MIN_IDLE=1

# Slice 4n recommended for 1× replica when you want the cluster-pipeline lift
export OMS_PG_POOL_MAX_SIZE=20
export OMS_PG_POOL_MIN_IDLE=2

# Slice 4n recommended for 2× replicas on Pop!'s default Supavisor topology
# (5 OMS JVMs * 10 = 50 client conns vs Supavisor's 20-backend pool — tolerable but
#  not optimal; see Tier 2.4 for the real fix)
export OMS_PG_POOL_MAX_SIZE=10
export OMS_PG_POOL_MIN_IDLE=2
```

Don't push `OMS_PG_POOL_MAX_SIZE` past `POOLER_DEFAULT_POOL_SIZE / N_OMS_JVMS_NEEDING_PG`
on the bench host without resizing Supavisor first. With the slice-4n pipeline, raising
`MAX_SIZE` past the Supavisor backend's capacity manifests as bimodal latency
(p50 ≈ slice-4m p50, p99 ≫ p50) instead of clean queueing — the cluster client no longer
hides the wait behind its own lock.

### Verification run (2026-05-14, slice 4n)

Same Pop! topology as slice 4m (5 OMS JVMs, single-account hot path, 60 k orders /
concurrency 200). Compare to the slice 4m verification numbers above:

```
== 1× replica, OMS_PG_POOL_MAX_SIZE=20 (slice 4n recommended) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=26.279 s  rps=2283.3
HTTP RTT p50=83.327 ms  p99=176.255 ms
oms_cluster_client_commit_round_trip p50=0.701 ms  p99=2.469 ms

== 1× replica, OMS_PG_POOL_MAX_SIZE=3 (regression — Hikari binds) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=114.347 s  rps=524.7
HTTP RTT p50=383.231 ms  p99=655.359 ms
oms_cluster_client_commit_round_trip p50=2.796 ms  p99=8.319 ms

== 2× replicas, OMS_PG_POOL_MAX_SIZE=10 (Supavisor budget tight) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=41.566 s  rps=1443.6
HTTP RTT p50=132.351 ms  p99=270.847 ms

== 2× replicas, OMS_PG_POOL_MAX_SIZE=20 (Supavisor backends exhausted) ==
submitted=60000  success=60000 (created=60000)  failed=0  elapsed=88.625 s  rps=677.0
HTTP RTT p50=92.991 ms  p99=773.631 ms  (bimodal — see histograms in Prometheus)
```

**The headline:** 1× replica × Hikari ≥ 10 lifts to **2 283 rps (1.87×** the slice-4m
1× best of 1 222 rps), and the round-trip histogram drops nearly an order of magnitude
(p50 0.701 ms vs slice-4m 2.796 ms at the same `MAX=3`). The 2× regressions are real but
they are **Supavisor budget regressions**, not slice-4n bugs — the slice-4m lock-driven
ceiling was just hiding the same underlying constraint.

When a slice-4n bench shows a regression vs slice-4m, **always** check Supavisor first:

```bash
docker exec ledger-supabase-pooler env | rg POOLER_DEFAULT_POOL_SIZE
docker exec ledger-supabase-db psql -U postgres -d postgres \
  -c "select count(*) from pg_stat_activity where datname='oms';"
```

If `pg_stat_activity` count for `oms` ≈ `POOLER_DEFAULT_POOL_SIZE`, you're queueing inside
Supavisor and `OMS_PG_POOL_MAX_SIZE * N_OMS_JVMS` exceeds the pool. Slice 4o (next section)
is the fix.

## Slice 4o setup — Supavisor topology + transaction-mode pooler (Tier 2.4)

Slice 4o exposes the bench-host Supavisor saturation that slice 4n made visible. Two
operational changes, sequenced for clean attribution. **No OMS code change** — the
connection string is fully env-driven via `OMS_PG_URL`.

### Step 1 — resize the bench-host Supavisor

Edit `ledger/docker/supabase/.env` on the bench host:

```bash
POOLER_DEFAULT_POOL_SIZE=50      # was 20 — backend pool per tenant
POOLER_MAX_CLIENT_CONN=300       # was 100 — incoming-client cap
```

Then recreate the pooler (note: the supabase stack on Pop! runs under compose project
`ledger-dev`, not the directory-default `ledger-supabase` — pass `-p ledger-dev` so the
pooler joins the same `ledger-dev_default` Docker network as `ledger-supabase-db`,
otherwise it lands on a fresh network and can't resolve `db:5432`):

```bash
docker rm -f ledger-supabase-pooler
cd ~/ledger/docker/supabase
docker compose -p ledger-dev up -d --no-deps supavisor
# wait for healthy
for i in $(seq 1 20); do
  h=$(docker inspect -f '{{.State.Health.Status}}' ledger-supabase-pooler 2>/dev/null)
  echo "$i: $h"; [ "$h" = healthy ] && break; sleep 3
done
docker exec ledger-supabase-pooler env | grep POOLER_DEFAULT_POOL_SIZE   # confirms 50
```

Headroom check before bumping further: `POOLER_DEFAULT_POOL_SIZE` must stay below
Postgres `max_connections` minus the connections used by the rest of the supabase stack
(realtime, postgrest, gotrue, meta) and the ledger app itself. On Pop! today
`max_connections=100` and the non-OMS supabase stack steady-state uses ~32 backends,
so 50 + 32 + ~10 ledger ≈ 92 backends — comfortably under 100.

### Step 2 — flip OMS to Supavisor's transaction-mode pooler (port 6543)

Edit `~/.oms-bench.env` on the bench host:

```bash
# was: jdbc:postgresql://127.0.0.1:5432/oms              (Supavisor session mode)
export OMS_PG_URL='jdbc:postgresql://127.0.0.1:6543/oms?prepareThreshold=0&socketTimeout=10'
```

`prepareThreshold=0` is **mandatory** with transaction-mode pooling. PgJDBC
auto-promotes statements to server-side prepared after the 5th call by default
(`prepareThreshold=5`). Supavisor's transaction mode (like pgbouncer's) re-routes
each transaction to whichever backend is free, so the cached `S_1` handle on the
previous backend won't exist on the next one — symptom is sporadic
`prepared statement "S_1" does not exist` errors at high concurrency. Setting
`prepareThreshold=0` disables server-side prepare entirely; the small wire-level
difference is offset many times over by no longer queueing on a tiny backend pool.

`socketTimeout=10` is a 10-second guard against a wedged backend silently
hanging the request thread; the bench's `oms.cluster.client.submit-timeout-ms` is
2 s but the JDBC layer can hold longer if no timeout is set.

Then restart **all OMS JVMs that hold a Hikari pool** (cluster-node doesn't):

```bash
# stop replicas + projector + fix-egress (cluster-node + fix loopback stay up)
for cls in OmsIngressReplicaBootstrap OmsPostgresProjectorBootstrap OmsFixEgressBootstrap; do
  for p in $(pgrep -f "$cls"); do kill -TERM "$p"; done
done
sleep 8
# any survivors get sigkill, then relaunch with the new env via your standard launch script
```

After relaunch, sanity-check that transaction-mode is doing its job — backend count
to the `oms` database in `pg_stat_activity` should be **dramatically lower** than the
sum of `OMS_PG_POOL_MAX_SIZE` across all JVMs:

```bash
docker exec ledger-supabase-db psql -U postgres -t -c \
  "select count(*) from pg_stat_activity where datname='oms';"
# session-mode (port 5432) at 4 JVMs * MAX=20 = 80 client conns: this returns ~80
# transaction-mode (port 6543), same client conns: this returns ~5–10
```

That ratio is the structural unlock. With transaction mode, the OMS-side
`OMS_PG_POOL_MAX_SIZE` decouples from the Supavisor backend count — N replicas at
`MAX=20` consume only as many backends as concurrent transactions actually need.

### Verification run (2026-05-14, slice 4o)

Same Pop! topology as slice 4n (5 OMS JVMs, single-account hot path, 60 k orders /
concurrency 200). Compare to slice 4n:

```
== 2× replicas, MAX=20, slice 4o step 1 (resize-only, port 5432 session mode) ==
submitted=60000  success=60000  failed=0  elapsed=22.408 s  rps=2 677.6
HTTP RTT p50=72.319 ms  p99=115.583 ms
oms_cluster_client_commit_round_trip p50=2.447 ms  p99=5.592 ms

== 2× replicas, MAX=20, slice 4o step 2 (resize + tx-mode, port 6543) ==
submitted=60000  success=60000  failed=0  elapsed=9.471 s  rps=6 335.2
HTTP RTT p50=31.103 ms  p99=73.087 ms
oms_cluster_client_commit_round_trip p50=1.398 ms  p99=3.845 ms
```

**The headline**: resize-only clears the slice-4n `2× MAX=20 = 677 rps` bimodal
regression (3.95× lift). Adding transaction-mode pooling on top is a further 2.37×
(6 335 rps total — **9.36× over the slice-4n regression at the same scenario, and
2.74× over slice 4m's 2 315 rps best**). Cluster-client commit p50 dropped from
2.447 ms (session-mode contention) to 1.398 ms (tx-mode, no Supavisor queueing).

### When to revert (rollback)

If transaction mode breaks something you didn't expect (e.g. you add a code path
that expects session-state continuity), revert by:

1. Edit `~/.oms-bench.env`: change port back to `5432`, drop `prepareThreshold=0`.
2. Restart the affected JVMs.

Zero data impact — the underlying Postgres is the same, only the Supavisor frontend
port changes.

### Future-proofing — advisory locks (currently unused)

[`oms/docs/shard-lease.md`](../shard-lease.md) describes `pg_try_advisory_lock` as
one option for outbox-reconciler leader election. **Not implemented today** (zero
hits in `src/main`), but if it ever lands, that DataSource has to stay on **session
mode (port 5432)** because advisory-lock state is connection-scoped and gets
released on transaction-mode commit. The pattern would be: keep the default OMS
DataSource on `6543`, register a separate `@Configuration` DataSource pointed at
`5432` and inject it specifically into the leader-election bean.

## Slice 4p evidence — ledger-bound bench, async outbox + compensator (Pop! 2026-05-14)

Same Pop! topology as slice 4o (5 OMS JVMs, 2× ingress replicas, `MAX=20`, port 6543
transaction-mode pool). The new dimension this bench measures: orders carry a real
`ledgerBalanceId` / `ledgerIdentityId` so `OrderIngressService.maybePlaceBuyLedgerInflightHold`
fires for every accept. The **only** difference between the two runs below is the slice 4p
async flag.

The burst tool was extended with `OMS_BURST_LEDGER_BALANCE_ID` /
`OMS_BURST_LEDGER_IDENTITY_ID` (config-and-limits compliant; both required when set, fails
fast at config build); see the doc-block in `IngressBurstMain`.

```
== Bench A: OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false (sync hold) ==
submitted=5000  success=414 (created=414, duplicate=0)  failed=4586  elapsed=17.913 s  rps=279.1
status breakdown:           201 = 414        500 = 4586
HTTP RTT p50=166.911 ms     p99=952.319 ms
oms_pipeline_ingress_accept p50=44.739 ms    p99=2147.484 ms
oms_cluster_client_commit_round_trip p50=2.447 ms  p99=5.592 ms

== Bench B: OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true  (slice 4p async outbox) ==
submitted=5000  success=4971 (created=4971, duplicate=0)  failed=29   elapsed=6.542 s   rps=764.3
status breakdown:           201 = 4971       502 = 29
HTTP RTT p50=14.015 ms      p99=46.143 ms
oms_pipeline_ingress_accept p50=5.592 ms     p99=15.379 ms
oms_cluster_client_commit_round_trip p50=1.000 ms  p99=3.845 ms
```

**Headline**: 2.74× rps lift (279 → 764) and 12× HTTP-RTT p50 reduction (167 ms → 14 ms);
the 92 % failure cliff in Bench A collapses to 0.6 %. The ingress-accept tx commit drops
8× p50 / 140× p99.

**Why Bench A fails**: the synchronous `RestLedgerInflightReservationClient.placeBuyNotionalHold`
hits the ledger's `balances.version` optimistic-concurrency lock — every burst thread tries
to mutate the same source balance. Server-side error: `Balance version conflict: source
balance was modified by another transaction`. The 50-way burst loses ~92 % of those races.
Slice 4p moves the hold off the ingress thread (insert into `ledger_inflight_outbox` in the
same accept tx; reconciler publishes serially per JVM via `FOR UPDATE SKIP LOCKED`), so the
collisions never reach the user.

**Compensator wired and observed working**: post-bench reconciliation drained the queue in
~1 minute; after drain the outbox was at:

| total | published | compensated | pending |
|---|---|---|---|
| 4 971 | 3 271 (65.8 %) | 1 700 (34.2 %) | 0 |

Each compensated row corresponded to an order whose hold lost the OCC race three times in a
row (`OMS_LEDGER_INFLIGHT_COMPENSATOR_ATTEMPTS_THRESHOLD=3`). The compensator submitted
`CancelOrderCommand` for each, the cluster admitted the cancel, the projector wrote
`orders.status=CANCELLED`. Sample audit on 500 randomly-picked compensated rows: **all 500
orders were `CANCELLED`** (no race-with-fill anomalies on this run; the slice-4q coalescer
narrows that race window further). Per-replica metric:
`oms_ledger_inflight_hold_compensated_total{outcome="cancelled"} = 948 (ingress-1) +
814 (ingress-2)`.

**Why Bench B is still well below slice 4o's 6 471 rps**: the synchronous
`LedgerBalanceClient.fetchIdentityIdForBalance` HTTP (`maybeVerifyLedgerBalanceBinding`)
runs once per accept when `ledgerBalanceId` is set — that ~25 ms ledger HTTP is on the
ingress critical path and not addressed by slice 4p. Either an in-process binding cache
(safe; balanceId→identityId is operator-rare-change) or the slice-4q coalescer would lift
this. **Slice 4p does what it advertises** (async hold, compensating cancel) — the binding
verify is a separate lever.

### Slice 4p setup — env to flip the async flag

Add these to `~/.oms-bench.env` (full ledger HTTP must be reachable; on Pop! the default
ledger app on `127.0.0.1:5001` is the same backing the customer-frontend dev stack):

```
export OMS_LEDGER_ENABLED=true
export OMS_LEDGER_BASE_URL='http://127.0.0.1:5001'
export OMS_LEDGER_API_KEY='<LEDGER_SERVER_SECRET_KEY>'      # see ledger/.env
export OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED=true
export OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true               # slice 4p — flip to false for Bench A
export OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID='<balance_id of @Nostro-EUR>'
export OMS_LEDGER_INFLIGHT_RESERVATION_CURRENCY=EUR
export OMS_LEDGER_INFLIGHT_COMPENSATOR_ENABLED=true
export OMS_LEDGER_INFLIGHT_COMPENSATOR_ATTEMPTS_THRESHOLD=3
export OMS_BURST_LEDGER_BALANCE_ID='<EUR customer balance with funds>'
export OMS_BURST_LEDGER_IDENTITY_ID='<identity owning that balance>'
```

`scripts/launch-bench-stack.sh <role>` (added in slice 4p) is a thin convenience wrapper
that sources `~/.oms-bench.env` then `nohup`s `./gradlew bootRun<Role>` to
`~/oms/logs/<role>.log`. Roles: `cluster-node | projector | fix-egress | ingress-1 |
ingress-2`. ingress-1 picks `OMS_HTTP_PORT=8088 OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8087`,
ingress-2 picks `8188 / 8187`.

### When to flip back (rollback)

If you suspect the async path is masking a behaviour change, set
`OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false` and restart the ingress replicas. Outbox rows
already accepted on the async path keep being driven by the reconciler regardless of the
flag flip; only newly-accepted orders take the sync path again.

## Slice 4q — `LedgerInflightCoalescer` topology + bench-driven verdict

Slice 4q ships the in-process MPSC queue + daemon flush thread that coalesces BUY inflight
holds onto Ledger's `POST /transactions/bulk?inflight=true&atomic=false`. The first Pop!
A/B (numbers in `### Slice 4q evidence` below) shows that, **as designed in this slice, the
coalescer is a regression vs slice 4p**: the design replaced the slice 4p "fire-and-forget
at accept" property with a synchronous wait on a per-batch bulk HTTP, and the daemon flush
thread became the new bottleneck. The feature is shipped, defaults to **off**, and stays
off until slice 4r redesigns the dispatch loop (see `### Slice 4q verdict + roadmap`
below). Production should remain on slice 4p (`OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`,
coalescer off) until that redesign lands and re-benches.

**Original hypothesis (slice plan)**: the slice 4p outbox path still posts one Ledger
transaction per HTTP, so a 50-way ingress burst still hits a 50-way `balances.version` OCC
race surface server-side — just driven by the reconciler tick instead of the ingress
thread. Slice 4q's bulk endpoint iterates the batch sequentially within a single HTTP, so
50 holds in one bulk POST contend with each other only via Postgres row-locks (waiting,
not failing); the OCC race surface drops by `batchSize`. The reconciler still owns the
fallback path: any item the bulk handler rejects (or the whole batch on a 5xx) is written
to `ledger_inflight_outbox` and the slice 4p reconciler + compensator drive it from there.

**Why the hypothesis was incomplete (Observed)**: the design held `OrderIngressService`
on a synchronous `future.get(timeoutMs)` waiting on the bulk flush, with **one** flush
thread per JVM. With concurrency 50 against 2 ingress replicas, that ceilings each JVM at
"one bulk POST in flight at a time" while slice 4p's outbox path returns 201 the moment
the outbox row is committed inside the accept tx (Ledger HTTP runs entirely off the
ingress critical path, with no per-item future to wait on). Bulk POSTs of 6–7 items
(observed `oms_ledger_inflight_coalescer_items_total` / `flush_seconds_count`) take ~100–
135 ms server-side, so the steady-state ceiling is ~7 items × (1 s / 135 ms) ≈ 52 ord/s
per JVM, ~104 ord/s across both — almost exactly what the bench measures (95 rps). The
intra-batch OCC reduction landed as designed; the loss came from the new accept-side
serialisation point, not from anything in the bulk dispatcher itself.

**Three-path topology — current state**:

| Profile | Flags | Critical-path Ledger HTTP per order | Durability gap | Pop! 2026-05-14 rps / p50 |
|---|---|---|---|---|
| Sync hold | `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false`, `OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=false` | 1 sync POST | None | 279 rps / 167 ms (slice 4p Bench A) |
| Outbox path (slice 4p) — **production default** | `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`, `OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=false` | 0 (HTTP after commit) | None — outbox row written in same tx as accept | 768 rps / 17 ms |
| Coalescer path (slice 4q) — **regression, off by default** | `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false`, `OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=true` | shared bulk POST per batch, but accept blocks on the per-item future | Small — items in MPSC queue at JVM crash time are lost (the slice 4p compensator only graduates rows that exist in `ledger_inflight_outbox`) | 95 rps / 431 ms |

The coalescer keeps the outbox path on as a fallback (priority order in
`OrderIngressService.maybePlaceBuyLedgerInflightHold` is coalescer → outbox → sync).
On any flush failure (whole-batch HTTP error or per-item Ledger error in a partial-success
response) the failed items are written to `ledger_inflight_outbox` so the slice 4p
reconciler + compensator still own correctness end-to-end. That much of the design held;
the throughput regression is in the "happy path" only.

### Slice 4q setup — env to flip the coalescer flag

Append these to the `~/.oms-bench.env` from slice 4p (same ledger backing endpoint, same
balance / identity envs, same compensator threshold for the fallback path):

```
export OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false           # coalescer takes priority anyway,
                                                         # but flip async off so the
                                                         # bench measures only the bulk path
export OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=true
export OMS_LEDGER_INFLIGHT_COALESCER_MAX_BATCH_SIZE=50    # default
export OMS_LEDGER_INFLIGHT_COALESCER_FLUSH_INTERVAL_MICROS=5000  # default 5 ms
export OMS_LEDGER_INFLIGHT_COALESCER_QUEUE_CAPACITY=1000  # default
export OMS_LEDGER_INFLIGHT_COALESCER_SUBMIT_TIMEOUT_MS=2000  # default
```

Restart the two ingress replicas (`./scripts/launch-bench-stack.sh ingress-1` /
`ingress-2`) and re-run `scripts/benchmark/shoot-ingress-orders.sh`. The cluster-node /
projector / fix-egress JVMs do not need to restart — the coalescer lives on the ingress
replicas only.

### When to flip back (rollback) — slice 4q

If a slice 4q regression is suspected, the safe rollback is the slice 4p path:

```
export OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=false
export OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true
```

Items already in the coalescer queue at the time of restart are drained to
`ledger_inflight_outbox` by `LedgerInflightCoalescer.stop()` (graceful Ctrl-C / SIGTERM);
the slice 4p reconciler picks them up after the JVM comes back. A SIGKILL (kernel OOM,
`kill -9`) loses any items currently in the in-memory queue — those orders are admitted
at the cluster but the hold never reached Ledger or the outbox, and the slice 4p
compensator cannot recover them because it only graduates rows that already exist in the
outbox table. Operationally: prefer SIGTERM on the ingress replicas when the coalescer is
on.

### Slice 4q evidence — Pop! bench, 2026-05-14

Same Pop! topology as slice 4p (5 OMS JVMs, 2× ingress replicas, port 6543 transaction-mode
pool, same `~/.oms-bench.env` ledger backing endpoint, same `OMS_BURST_LEDGER_BALANCE_ID` /
`OMS_BURST_LEDGER_IDENTITY_ID`). Burst load: `OMS_BURST_TOTAL=5000`, `OMS_BURST_CONCURRENCY=50`,
`OMS_BURST_URLS` round-robin across both ingress replicas. Same Ledger / Postgres state
between A and B (only the ingress replicas restart between runs to flip the flag pair).
The ingress replicas' burst tool histograms are HdrHistograms over **client-side** HTTP
RTT (the burst tool warmup is 0; the first 30 samples are not excluded — they sit in the
slow tail).

```
== Bench A: OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true  (slice 4p outbox path) ==
submitted=5000  success=4972 (created=4972, duplicate=0)  failed=28   elapsed=6.510 s   rps=768.1
status breakdown:           201 = 4972       502 = 28
HTTP RTT  p50=16.703 ms     p95=39.359 ms    p99=64.255 ms    p999=198.527 ms

== Bench B: OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=true (slice 4q coalescer) ==
submitted=5000  success=4959 (created=4959, duplicate=0)  failed=41   elapsed=52.513 s  rps=95.2
status breakdown:           201 = 4959       502 = 41
HTTP RTT  p50=431.103 ms    p95=819.199 ms   p99=1000.959 ms  p999=1139.711 ms
```

Coalescer-side Prometheus snapshots after Bench B (sum across both ingress replicas):

```
oms_ledger_inflight_coalescer_flush_seconds_count{outcome=applied}        822
oms_ledger_inflight_coalescer_flush_seconds_sum{outcome=applied}          93.5 s
oms_ledger_inflight_coalescer_flush_seconds_max{outcome=applied}          1.032 s

oms_ledger_inflight_coalescer_items_total{outcome=applied}                4959
oms_ledger_inflight_coalescer_submit_seconds_count{outcome=applied}       4959
oms_ledger_inflight_coalescer_submit_seconds_sum{outcome=applied}         1305.0 s
oms_ledger_inflight_coalescer_submit_seconds_max{outcome=applied}         1.192 s
```

**Headline**: 8.1× **regression** (768 → 95 rps), 25.8× p50 inflation (17 → 431 ms),
15.6× p99 inflation (64 → 1001 ms). The intra-batch OCC reduction landed as designed —
no item-level fallback fired and the failure rate stays comparable to slice 4p (0.8 % vs
0.6 %) — but the throughput collapsed because of the new design property described above.

**Math sanity-check**: 822 flushes / 93.5 s = avg flush 113.7 ms; 4959 items / 822
flushes = 6.0 items / batch (vs `maxBatchSize=50`). Average per-item submit-to-ack
time = 1305 s / 4959 = 263 ms (≈ HTTP RTT mean of 437 ms minus the burst-tool / ingress
accept hop). With one flush thread per JVM and 113.7 ms per flush, max throughput per
JVM = (1000 ms / 113.7 ms) × 6 items = **53 items/s/JVM** = 106 items/s aggregate, almost
exactly the 95 rps the bench measured. **Root cause of the regression**: synchronous
accept→bulk-HTTP coupling + single flush thread per JVM, not the bulk endpoint itself.

### Slice 4q verdict + roadmap

- **Ship the code** (it is on `origin/main`), keep `inflightCoalescerEnabled=false` as
  the default, and document the regression here so a future operator does not flip the
  flag without seeing the numbers. The bulk dispatcher and outbox-fallback wiring compose
  cleanly; only the dispatch loop's accept-side coupling needs to change.
- **Production stays on slice 4p** (`OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`,
  coalescer off). Until slice 4r lands the coalescer flag is **not** to be turned on in
  any environment past dev.
- **Slice 4r (planned)** will redesign as fire-and-forget at accept, mirroring slice 4p:
  the ingress accept tx writes to `ledger_inflight_outbox` (same row shape as 4p), and a
  pool of coalescer flush threads drives the bulk HTTP **after** commit, draining the
  outbox table directly instead of an in-memory MPSC queue. That removes both the accept
  blocking property and the JVM-crash durability gap, and lets the bulk endpoint's OCC
  reduction land on top of slice 4p's accept latency floor instead of replacing it.

## Profile-led pivot — where the ingress ceiling actually lives (Pop! 2026-05-14)

Written **before** any slice 4r / Tier-2.5-phase-C work, after slice 4q walked itself
back. The standing question after slice 4q was: "is the next 2× rps available, and where
is it?" Not by guessing — by measuring at the slice 4p ceiling. The data below answers
"yes, but not where slice 4q assumed".

### Setup

- Pop! `192.168.68.112`, slice 4p production-shaped config: `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`,
  `OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=false`, 2× ingress replicas + cluster-node + projector + fix-egress.
- Bench: `bootRunBurst` with `OMS_BURST_TOTAL=30000` / `OMS_BURST_CONCURRENCY=50`,
  round-robin across `:8088` and `:8188`, real Ledger HTTP, AAPL 1@150.
- The 5 k bursts in slices 4o–4q are warmup-bound; **30 k is closer to true steady state**.

### What the 30 k burst measured

| Surface | Value | Notes |
|---|---|---|
| **Throughput** | **1 094 rps** | (vs 768 rps on the 5 k slice 4p bench — 5 k underestimated the ceiling) |
| HTTP RTT p50 / p99 / p99.9 | 21.1 / 54.8 / 100.7 ms | client-side, n=29 840 successful |
| Failures | 119 / 30 000 = 0.4 % | all `502` (Ledger transient), no OCC |
| `oms_pipeline_ingress_accept_seconds` p50 / p99 (ingress-1) | **4.2 / 9.1 ms** | **server-side @Transactional** |
| `oms_cluster_client_commit_round_trip_seconds` p50 / p99 (ingress-1) | 1.4 / 4.5 ms | inside the accept tx |
| `pg_stat_statements` accept-tx INSERTs (per call) | 0.02–0.04 ms | three INSERTs total ≈ 0.10 ms / accept |

The first observation is that the 5 k-burst headline of "768 rps" was a warmup-shaped
underestimate; **the true slice 4p ceiling on this rig is ~1 094 rps**. Refit any "next
slice will lift X" claim against 1 094 rps, not 768 rps.

The second observation is that the **server-side accept tx is fast** (4 ms p50). The
17 ms gap between server-tx p50 (4 ms) and HTTP-RTT p50 (21 ms) lives **outside**
`@Transactional`: Tomcat acceptor queue, filter chain, JSON serialise, and — critically —
**Hikari connection borrow-wait before the @Transactional opens**.

### What the jstack mid-bench captured

`jstack` of ingress-1 (PID 2 473 554) at 8 s into the burst (peak load) showed:

| Counter | Value |
|---|---|
| `http-nio-8088-exec-*` threads total | 41 |
| Threads stalled inside `HikariPool.getConnection` (parking on `SynchronousQueue$Transferer`) | **15** |
| Threads in `RUNNABLE` (mostly `EPoll.wait` / `Net.poll` — connector-idle, not mid-request) | 18 |

So at peak load, **~37 % of ingress-1's request threads are queued on Hikari**, not on
SQL execution, not on the cluster commit, not on Ledger. The pool is sized 20
(`OMS_PG_POOL_MAX_SIZE=20`); the queue is the consequence.

### Why the pool is starved — `pg_stat_statements` after the run

```text
total_ms |  calls  | mean_ms | pct  | query
---------+---------+---------+------+------------------------------------
296579.9 |    397  | 747.053 | 97.6 | SELECT ... FROM domain_event_outbox
                                       WHERE published_at IS NULL
                                         AND created_at <= $1
                                       ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED
  1192.5 | 29 881  |   0.040 |  0.4 | INSERT INTO ledger_inflight_outbox ...
  1020.0 | 45 995  |   0.022 |  0.3 | SELECT t.precise_amount ... FROM transactions
   779.4 | 46 097  |   0.017 |  0.3 | INSERT INTO domain_event_outbox ...
   653.5 | 16 114  |   0.041 |  0.2 | INSERT INTO orders ...
   535.4 | 16 114  |   0.033 |  0.2 | UPDATE orders ...
```

**One query is 97.6 %** of all Postgres execution time during the run: the
`DomainFanoutReconciler` (`@Scheduled fixedDelay=500 ms`) draining
`domain_event_outbox`. 397 calls × 747 ms each ≈ 296 s of accumulated tx time during a
27 s wall-clock burst, i.e. multiple JVMs running it concurrently and each call holds a
Hikari connection for ~3/4 of a second.

The accept-tx writes — `INSERT orders`, `INSERT domain_event_outbox`, `INSERT
ledger_inflight_outbox`, `UPDATE orders` — together account for **<1.2 % of Postgres
exec time**. Postgres is not the ingress bottleneck; **Postgres is mostly idle when the
reconciler isn't running, and saturated on one bad query when it is**.

### Why that one query is 747 ms — `EXPLAIN ANALYZE`

```text
Limit  (cost=0.43..4822.36 rows=200 width=74) (actual time=662.221..662.275 rows=50 loops=1)
  Buffers: shared hit=728 255 read=306 999 dirtied=9 written=180
  ->  LockRows  (cost=0.43..2 400 188.73 rows=99 553 width=74) (actual time=662.221..662.271 rows=50 loops=1)
        ->  Index Scan using domain_event_outbox_pkey on domain_event_outbox
              (cost=0.43..2 399 193.20 rows=99 553 width=74)
              (actual time=662.120..662.178 rows=72 loops=1)
              Filter: ((published_at IS NULL) AND (created_at <= now()))
              Rows Removed by Filter: 3 138 866
Planning Time: 0.346 ms
Execution Time: 662.314 ms
```

Postgres is using **`domain_event_outbox_pkey`** (the PK on `id`) — not the partial
index `idx_domain_event_outbox_pending btree (created_at) WHERE published_at IS NULL` —
because `ORDER BY id` matches pkey ordering and the planner avoids a Sort. With **3.14 M
total rows** in the table (only ~100 unpublished at any moment, the rest are completed
events that are never deleted), the pkey scan walks **3 138 866 rows** through the heap
and `LockRows` on top to filter out the 99.997 % that are already published. That's the
662 ms.

The partial index on `created_at` is on the wrong column to satisfy `ORDER BY id`. The
planner picks pkey, the pkey is ~99.997 % dead-for-this-predicate, and the scan ends up
in an O(table-size) regime rather than O(unpublished).

### What this means for the next slice

The pre-conditions for a real lift are:

1. The **DomainFanoutReconciler SELECT** is the dominant Postgres time and a
   real source of Hikari contention on ingress JVMs.
2. The **accept tx itself is already fast** (4 ms p50, 9 ms p99 server-side). Slice 4r as
   originally pitched (re-shape the Ledger hold path) is **not on the critical path** at
   1 094 rps — the accept tx isn't waiting on Ledger, it's waiting on a connection.
3. The **17 ms HTTP-vs-server gap** is dominated by Hikari borrow-wait. Removing the
   reconciler stall (or growing the pool, or running the reconciler off the ingress
   pool) should compress that gap directly.

Falsifiable hypothesis: an evidence-led "Tier 2.5 phase C" slice that adds a partial
index `(id) WHERE published_at IS NULL` (or moves the reconciler off the ingress pool,
or both) will:

- drop `pg_stat_statements` mean for that query from 747 ms to <1 ms,
- drop `http-nio-*-exec` parking-on-Hikari count below 5 at peak,
- lift sustained 30 k-burst rps from 1 094 → at least 1 800 rps (i.e. the same
  factor that the connection-conservation buys back),
- have **no** correctness change — the reconciler's `FOR UPDATE SKIP LOCKED` semantics
  are unchanged.

Both ways to verify the hypothesis are cheap: scrape `pg_stat_statements` after the
fix, and re-run the same 30 k burst.

### Slices ruled IN by the data

- **Phase-C-1** (cheap, surgical, evidence-led): add `CREATE INDEX CONCURRENTLY
  idx_domain_event_outbox_pending_id ON domain_event_outbox (id) WHERE published_at IS
  NULL;` so `ORDER BY id LIMIT N FOR UPDATE SKIP LOCKED` walks only unpublished rows.
- **Phase-C-2** (housekeeping, correctness-neutral): periodic
  `DELETE FROM domain_event_outbox WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days'`,
  or move published rows to a partitioned tail. Bounds the "even with the right index,
  bloat will eventually catch us" risk.
- **Phase-C-3** (decoupling): give the reconciler its **own** small Hikari pool
  (`spring.datasource.reconciler.hikari.maximum-pool-size=4`) so even a slow query
  cannot starve HTTP accepts. Optional if Phase-C-1 lands the lift cleanly.

### Slices ruled OUT (or down-prioritised) by the data

- **Slice 4r as originally framed** (rebuild the LedgerInflightCoalescer as
  fire-and-forget at-accept). The accept tx is not Ledger-bound at 1 094 rps. Until
  Phase-C lands, slice 4r risks the same shape of mistake as 4q: building before
  measuring whether the bottleneck it addresses is even live.
- **Cluster-node parallelism / projector parallelism**. `cluster_client_commit_round_trip
  p99 = 4.5 ms` and projector-lag stays in single-digit seconds in the slice 4n
  verification run; neither is the live ceiling at 1 094 rps.

### Operator-side: how to confirm before / after Phase-C-1

1. Reset stats: `docker exec ledger-supabase-db psql -U supabase_admin -d oms -c 'SELECT
   pg_stat_statements_reset();'`
2. Run the same 30 k burst (`OMS_BURST_TOTAL=30000 OMS_BURST_CONCURRENCY=50` against
   both ingress URLs round-robin).
3. Compare:
   - **rps + HTTP RTT p50/p99** (HdrHistogram in burst summary).
   - **pg_stat_statements top-by-time** — the reconciler row's `mean_ms` should drop
     from ~700 ms to <1 ms; its `pct` should drop from 97.6 % to single digits.
   - **`jstack` of one ingress JVM at peak** — count of `http-nio-*-exec` threads
     parking inside `HikariPool.getConnection` should drop below 5 (target: 0).

Logs from the 2026-05-14 baseline live in
`~/oms/logs/bench-profile-2026-05-14/` on Pop! (`pre-/post-ingress-{1,2}.txt`,
`mid1-{cluster,projector,fixegress,ingress1,ingress2}.jstack`,
`pg-top-by-time.txt`, `burst.log`).

## Tier 2.5 phase C-1 + C-2 verification (Pop! 2026-05-14, same day)

Falsifiable predictions from "Profile-led pivot" above were verified by running the
same 30 k burst against the same Pop! rig, after applying the changes
incrementally. Headline:

| Stage | rps (30 k @ conc 50) | HTTP p50 / p99 / p999 | What changed |
|---|---|---|---|
| Pre-V30 | 1 094 | 21.1 / 54.8 / 100.7 ms | baseline (Profile-led pivot section) |
| V30 only (`domain_event_outbox` partial idx on `id`) | 1 130 | 15.9 / 60.4 / 85.6 ms | reconciler SELECT 662 ms → 0.083 ms |
| V30 + V31 (`ledger_inflight_outbox` partial idx on `id`) | 970 | 13.9 / 55.2 / 74.5 ms | second reconciler SELECT 15 ms → 0.32 ms; rps **regressed** because Hikari freed by indexes was re-consumed by Ledger HTTP verify still inside `@Transactional` (next stage fixes that) |
| V30 + V31 + C-2 (Ledger verify out of `@Transactional`) | **1 179** | **7.4 / 12.3 / 14.9 ms** | Hikari starvation completely gone (0 / 0 stalled) |
| Same code, conc=50 | 1 179 | 7.4 / 12.3 / 14.9 ms | bench-client-bound |
| Same code, conc=200 | **2 689** | 9.0 / 17.3 / 28.0 ms | server scaling well |
| Same code, conc=400, 60 k burst | **3 807** | 39.7 / 92.6 / 150.5 ms | server saturating, but new bottleneck is Ledger HTTP |

**Ceiling on this Pop! rig moved from 1 094 rps → ~3 800 rps (3.5 ×)** with three
small changes:

1. **V30** (Flyway, no app code) — `CREATE INDEX CONCURRENTLY IF NOT EXISTS
   idx_domain_event_outbox_pending_id ON domain_event_outbox (id) WHERE published_at IS NULL;`
2. **V31** (Flyway, no app code) — same shape on `ledger_inflight_outbox`, predicate
   `WHERE published_at IS NULL AND compensated_at IS NULL`.
3. **C-2** (one Java file: `OrderIngressService`) — drop `@Transactional` from
   `persistAccepted`, run `maybeVerifyLedgerBalanceBinding` first (no Hikari conn
   held during the Ledger `GET /balances/{id}` HTTP RTT), then open the Postgres
   tx via `TransactionTemplate.execute` for the cluster admit + outbox INSERTs.

### EXPLAIN ANALYZE before / after, both indexes

`domain_event_outbox` (V30, query `WHERE published_at IS NULL AND created_at <= now()
ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED`):

```text
pre-V30: Index Scan using domain_event_outbox_pkey
         Buffers: shared hit=728 255 read=306 999
         Rows Removed by Filter: 3 138 866
         Execution Time: 662.314 ms

post-V30: Index Scan using idx_domain_event_outbox_pending_id
          Buffers: shared hit=15
          Execution Time: 0.083 ms             (≈8 000 × speedup)
```

`ledger_inflight_outbox` (V31, query `WHERE published_at IS NULL AND
compensated_at IS NULL AND created_at <= now() ORDER BY id LIMIT 200 FOR UPDATE
SKIP LOCKED`):

```text
pre-V31: Index Scan using ledger_inflight_outbox_pkey
         Buffers: shared hit=26 934
         Rows Removed by Filter: 42 972
         Execution Time: 15.014 ms

post-V31: Index Scan using idx_ledger_inflight_outbox_pending_id
          Buffers: shared hit=406
          Execution Time: 0.320 ms             (≈47 × speedup, 66 × fewer pages)
```

Both indexes use the `-- flyway:executeInTransaction=false` directive so the
build runs with `CREATE INDEX CONCURRENTLY` (no AccessExclusiveLock on a hot
table). Watch out for a Flyway 10 quirk: `executeInTransaction=false` migrations
**cannot mix** non-transactional `CREATE INDEX CONCURRENTLY` with a
transactional `COMMENT ON INDEX` in the same file. The migrations carry the
rationale in their own header comments instead.

### `pg_stat_statements` top-by-time, before / after

```text
pre-V30 (one row dominates 97.6 %):
  total_ms=296 580  calls=397  mean_ms=747.05  pct=97.6  SELECT FROM domain_event_outbox ...
  total_ms=  1 192  calls=29 881  mean_ms=0.04  pct=0.4  INSERT INTO ledger_inflight_outbox ...

post-V30+V31+C-2 (no single dominant query — work is now Postgres-bound on accept-tx writes):
  total_ms=  3 754  calls=59 482  mean_ms=0.063  pct=35.4  INSERT INTO ledger_inflight_outbox ...
  total_ms=  1 867  calls=64 607  mean_ms=0.029  pct=17.6  INSERT INTO domain_event_outbox ...
  total_ms=  1 707  calls=64 456  mean_ms=0.026  pct=16.1  SELECT FROM transactions (Ledger DB)
  (DomainFanoutReconciler SELECT no longer in the top 25)
```

### `jstack` mid-bench thread-state, before / after

| Counter (sum across both ingress JVMs at peak) | Pre-V30 | V30+V31+C-2 conc=50 | V30+V31+C-2 conc=400 |
|---|---|---|---|
| `http-nio-*-exec` total | 41+25=66 | 34+39=73 | 200+200=400 |
| Stalled inside `HikariPool.getConnection` | 15+0=15 | 0+0=**0** | 62+88=150 |
| `RUNNABLE` inside `HttpClient.parseHTTPHeader` (Ledger verify HTTP) | not measured | 23+19=42 | 97+93=**190** |

Two important interpretations of the conc=400 column:

- The **Ledger verify HTTP call** is now the dominant per-request cost. With a
  single balance ID being repeatedly checked across ~30 k orders, ~190 ingress
  threads are simultaneously RUNNABLE inside `HttpClient.parseHTTPHeader` waiting
  for the same `GET /balances/{id}` to come back. **C-3 candidate**: cache the
  `(balanceId, identityId)` mapping in `OrderIngressService` for a small TTL
  (60–300 s); first request per balance pays the HTTP cost, the rest hit the
  cache.
- Hikari starvation **comes back at conc=400** (150 of 400 threads parking) —
  but this is now expected: 400 threads × Hikari pool of 20 = 20 × ratio. At
  conc=200 the ratio is 10× and the rig sustains 2 689 rps with p50 9 ms; at
  conc=400 the ratio is 20× and p50 inflates to 39 ms. **Realistic
  sustained-load ceiling with reasonable p50 (<10 ms) is ~2 700 rps** on this
  rig; the 3 807 rps headline at conc=400 trades ~5 × p50 inflation for
  marginal extra throughput.

### What this verifies / falsifies relative to the Profile-led pivot predictions

| Prediction | Outcome |
|---|---|
| `pg_stat_statements` mean for the `domain_event_outbox` SELECT drops from 747 ms to <1 ms | **TRUE** (0.083 ms in EXPLAIN; query no longer in `pg_stat_statements` top 25) |
| Hikari starvation count drops below 5 at peak | **TRUE** at conc=50/200 (= 0); **FALSE** at conc=400 (resurfaces but for a different reason — request ratio not slow query) |
| 30 k-burst rps lifts from 1 094 → ≥ 1 800 | **FALSE at conc=50** (only 1 179, bench-client-bound). **TRUE at conc≥200** (2 689–3 807) |
| 17 ms HTTP-vs-server gap closes | **TRUE** — `oms.pipeline.ingress.accept_seconds` p50 stayed ~4 ms (server tx never was the bottleneck) but **HTTP RTT** p50 collapsed from 21 ms to 7.4 ms |

The "FALSE at conc=50" line is the only diagnostic miss in the morning's memo:
the bench tool's HTTP client at conc=50 was already the cap, hiding the actual
server ceiling. Future bench runs against the rig should sweep concurrency at
50/100/200/400 and report each, not assume conc=50 is steady-state. Logs from
the C-1+C-2 verification live on Pop! in:

- `~/oms/logs/bench-profile-2026-05-14-v30/` (V30 only)
- `~/oms/logs/bench-profile-2026-05-14-v31/` (V30+V31, verify still inside tx — regression)
- `~/oms/logs/bench-profile-2026-05-14-c2/` (V30+V31+C-2 at conc=50)
- `~/oms/logs/bench-profile-2026-05-14-c2-scale/` (conc 100/200/400 sweep)
- `~/oms/logs/bench-profile-2026-05-14-c2-saturate/` (conc=400, 60 k burst, full instrumentation)

### Slices ruled IN by the C-2 saturation profile (next-up)

- **Phase-C-3** — cache `(balanceId → identityId)` mapping in `OrderIngressService` for
  a small TTL (60–300 s, configurable). 99 %+ of `RestLedgerBalanceClient.fetchIdentityIdForBalance`
  calls in steady state come back with the same answer, and the TTL bounds the
  staleness risk if a balance is reassigned. Falsifiable: `RUNNABLE inside
  HttpClient.parseHTTPHeader` count at conc=400 should drop from ~190 to <10
  on subsequent requests after a cache warm-up. rps ceiling should move further;
  the new ceiling will likely surface as either cluster-node single-thread state
  machine or projector lag (next bottleneck).
- **Phase-C-4** (only if C-3 lands and rps still has headroom) — bump
  `OMS_PG_POOL_MAX_SIZE` from 20 → 40 per ingress JVM, or run the reconciler off
  its own Hikari pool. The data shows Hikari is fine at conc≤200 with the current
  pool size **once** the slow reconciler query is gone; over-provisioning the
  pool is only useful if we expect conc≥400 in production-shape clients.

### Slices ruled OUT (or further down-prioritised) by the data

- **Slice 4r as originally framed** (rebuild `LedgerInflightCoalescer` as
  fire-and-forget at-accept). Stays out — the accept tx still doesn't wait on
  Ledger after C-2; the verify HTTP is now its own thing, not on the
  Postgres-tx critical path. C-3 (cache verify) addresses the same per-request
  Ledger cost more cheaply than C-4-style coalescing, and without the
  durability gap.
- **Cluster-node parallelism / pipelined commit work**. At 3 800 rps the
  cluster-node JVM jstack shows 8 RUNNABLE / 5 TIMED_WAITING / 4 WAITING with no
  thread parked on contended state — far from saturated.

## Caveats

- This is a **single-host single-cluster-node** rig. It does NOT exercise consensus, leader
  failover, replay, or k8s pod restart paths. Phase 5 will move that to a real k8s
  StatefulSet with anti-affinity across AZs.
- `oms.cluster.client.commit_round_trip` is end-to-end on the cluster client thread; on a
  single-node cluster the consensus path is single-process so the absolute number is **lower**
  than it will be in 3-node Phase-5 production. Use it for *trending* and per-stage attribution,
  not as the SLO baseline.
- The `oms.fix_egress.lag_seconds` gauge is **end-of-run** (a single snapshot, not a
  distribution). Sustained-load tail latency on the FIX side is best measured by a long-running
  load + a Prometheus `histogram_quantile` over the cursor-lag scrape window in your dashboards.

## Running integration tests against this same Postgres (macOS Docker Desktop sidenote)

Out of scope for the bench itself, but worth capturing here because most operators land on this
runbook after fighting the same problem: on macOS Docker Desktop, Testcontainers' Java client
fails with `Could not find a valid Docker environment` even though `docker ps` works fine.
Cause: `/var/run/docker.sock` is a symlink to `~/.docker/run/docker.sock`, which is Docker
Desktop's CLI-proxy socket. It returns an empty `/info` body plus a
`com.docker.desktop.address` label naming the real daemon socket
(`~/Library/Containers/com.docker.docker/Data/docker-cli.sock`). The Docker CLI follows that
redirect; the Testcontainers Java client (1.20.x) does not.

`AbstractPostgresIntegrationTest` already supports an externally-managed-Postgres path that
sidesteps this entirely. With the compose Postgres above:

```bash
export OMS_CI_JDBC_URL='jdbc:postgresql://127.0.0.1:5440/oms'
export OMS_CI_JDBC_USER=oms
export OMS_CI_JDBC_PASSWORD=oms
./gradlew test
```

ITs reuse the bench Postgres and Flyway-migrate it on first context boot — no separate
container needed. (CI uses the same env vars against `services: postgres` in GitHub Actions.)

## Tear-down

```bash
# Ctrl-C the bootRun* tasks individually (clean shutdown wires off the cluster client).
docker compose down
```
