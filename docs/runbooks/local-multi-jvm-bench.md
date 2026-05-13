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
concurrency-bound at ~1.2 krps before adding network / Tomcat overhead. With
`OMS_PG_POOL_MAX_SIZE` raised (and Supavisor either resized or bypassed via direct
Postgres), each replica should clear ~3 krps and the same 1.89× scaling factor holds.

- **Cost / risk**: none, beyond the Supavisor session pool which is the next item.
- **Where**: `application-oms-ingress-replica.yaml`. Each replica needs a unique
  `OMS_HTTP_PORT` and a unique `OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT`. The
  cluster-client `ingress-channel` / `egress-channel` defaults are
  `aeron:udp?endpoint=localhost:0` (port 0 = ephemeral) so two replicas sharing one Aeron
  media driver each get distinct ports automatically (verified: replica 2 booted with
  default channels and connected to the cluster on the same media driver as replica 1
  without any port collision).

### Tier 2 — Move buying-power hold off the ingress critical path

Today the buying-power / pre-trade reservation is a synchronous JDBC call to the ledger
inside the ingress acceptor's HTTP request thread. That is the biggest single chunk of the
~3 ms ingress-accept p50. Two options, in order of preference:

1. **Pipeline / batch the holds**. Multiple in-flight orders share a single ledger
   transaction window (group commit on the OMS side). Concretely: replace the
   "1 HTTP request → 1 hold tx" mapping with a small in-process queue + group-coalesce
   worker that flushes every ≤1 ms or every 32 orders. Each order's HTTP response waits on
   its own hold result; the *batch* commits to ledger as one tx. Ledger throughput per
   commit is roughly fixed-cost, so this is a 4–10× lift on the hold tier alone.
   - Lift: ingress p50 drops from ~3 ms to ~0.5–1 ms; ingress ceiling moves from ~3.7 krps
     to ~10–20 krps before cluster ingress becomes the next bottleneck.
   - Risk: low — semantics are preserved (hold is still synchronous from the client's POV).
2. **Cache balances locally with optimistic reservation**. Ingress holds against an
   in-process balance projection (Redis-backed or built off the ledger event stream),
   submits the order to the cluster, and reconciles asynchronously. Mismatches reject the
   order out-of-band (cancel + reverse hold). Only worth doing if (1) doesn't reach
   target, because it changes the consistency model. The ledger already publishes the
   stream we'd consume.

- **Where**: `OrderAcceptService` / `LedgerHoldClient` (or whatever the slice 3 ledger
  client is named in `customer-frontend/`-style usage). The batching boundary fits
  cleanly between the request-scoped hold and the JDBC tx.
- **Slice**: 4n (proposed).

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
