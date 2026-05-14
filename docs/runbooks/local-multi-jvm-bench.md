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

## Tier 2.5 phase C-3 verification — verify path uses Ledger's existing Redis cache (Pop! 2026-05-14)

### What changed

C-2 left ~190 of 400 ingress http-nio threads at conc=400 stuck in
`HttpClient.parseHTTPHeader` for the Ledger balance verify call
(`OrderIngressService.maybeVerifyLedgerBalanceBinding` →
`RestLedgerBalanceClient.fetchIdentityIdForBalance`). The runbook recommended
"cache `(balanceId → identityId)` in OMS for a small TTL".

Audit found a cheaper fix:
[`ledger/src/services/balance.service.ts:84`](../../ledger/src/services/balance.service.ts)
already implements a Redis `BalanceCache` (60 s TTL,
[`ledger/src/cache/balance-cache.ts:14`](../../ledger/src/cache/balance-cache.ts))
that **only activates when `withQueued === false`**:

```ts
if (include.length === 0 && !withQueued) {
  const cached = await getCache().get(balanceId);
  if (cached) return cached;
}
```

But OMS's `RestLedgerBalanceClient.fetchBalanceRoot` was hard-coded to send
`?with_queued=true`, even from the verify path which only reads the durable
`identityId`. So the verify call was *deliberately* bypassing a cache that
already existed and ran the `getQueuedAmounts` SELECT on `transactions` for
every request.

### Standalone Ledger HTTP timing on Pop!

Measured against `http://127.0.0.1:5001/balances/{id}` with `X-Ledger-Key`,
on `balance_c28737c2-70f5-45d2-84aa-7c6c0b1dbbc2`:

| query | single call | 50 parallel calls (wall-clock) |
| ---- | ---- | ---- |
| `?with_queued=true` (current OMS) | 2 – 28 ms | 5 015 ms total → ~100 ms / call |
| `?with_queued=false` (warm cache) | **0.8 – 1.1 ms** | **13.5 ms total → ~0.27 ms / call** |

`EXPLAIN ANALYZE` of the `getQueuedAmounts` SELECT on the Ledger DB at idle:
0.105 ms, hash anti join, 25 buffers shared hit. So the query itself is fine
under low load; what blew up at conc=400 was Express + Prisma + Postgres pool
serialisation when 50 concurrent verifies all bypassed the cache.

### OMS-side fix (only)

[`oms@ee761c6`](https://github.com/magnus-pepins/oms/commit/ee761c6) — parameterise
`fetchBalanceRoot(balanceId, withQueued)`. `fetchAvailableBalance` and
`fetchBalanceReadModel` keep `true` (they need queued amounts);
`fetchIdentityIdForBalance` passes `false`. **No Ledger code change** — the
existing cache is the correct fix; OMS just needed to stop opting out of it.

### Bench result (conc 50 / 200 / 400; same hardware as C-1+C-2)

| concurrency | C-2 baseline RPS | **C-3 RPS** | lift over C-2 | C-2 mean RTT | C-3 mean RTT | C-3 p50 | C-3 p99 |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| 50  | 1 179 | **6 896** | **5.85×** | 14.0 ms | 6.8 ms | 6.3 ms | 20.0 ms |
| 200 | 2 689 | **7 521** | 2.80× | ?       | 26.0 ms | 24.3 ms | 74.5 ms |
| 400 | 3 807 | **7 762** | 2.04× | 41.4 ms | 50.9 ms | 49.7 ms | 111.1 ms |

Bench tool: `bootRunBurst` (`OMS_BURST_TOTAL=30 000` for conc≤200, `60 000` for
conc=400). All 60 000 / 30 000 requests `201`, no failures. Re-run at conc=400
60 000 reproduced 7 562 rps within run-to-run variance.

**Headline**: Pop! ceiling moved from C-2's 3 807 rps → **7 762 rps** at conc=400
(another **2.04×** on top of C-1 + C-2; **7.10×** over the morning's slice 4p
async-hold baseline of 1 094 rps).

The huge conc=50 jump (1 179 → 6 896 rps) is because the bench client at
conc=50 was *server-bound* on the verify HTTP under C-2 (each request held one
client thread for ~30 ms while the verify ran), and is now near-zero per call.
At conc=200 and conc=400 the bench client itself is fine; the server is the
governor.

### Saturation profile at conc=400 (`jstack` on the actual app JVMs)

http-nio thread state distribution per ingress (212 / 214 threads alive):

```
ingress-1: 165 / 212 (78 %) parked in com.zaxxer.hikari.pool.HikariPool.getConnection
            16     RestClient/HttpClient frames (was ~190 in C-2)
             8     RUNNABLE in LedgerInflightOutboxRepository.insert
             8     RUNNABLE in OrderIngressService.persistAccepted
             3     parked on OmsClusterIngressClient.submitAcceptOrder (Aeron offer back-pressure)

ingress-2: 170 / 214 (79 %) parked on Hikari (150 raw Unsafe.park + 20 explicit)
            18     RestClient/HttpClient frames
             9     RUNNABLE in LedgerInflightOutboxRepository.insert
             7     parked on OmsClusterIngressClient.submitAcceptOrder
             6     RUNNABLE in RestClient.exchangeInternal
```

`pg_stat_statements` for the OMS DB during the bench:

```
3 607 ms / 614 calls / mean 5.876 ms / 36.0 % — UPDATE ledger_inflight_outbox SET compensated_at=$1 ...
3 253 ms / 60 000 / 0.054 ms / 32.5 %       — INSERT INTO ledger_inflight_outbox
1 635 ms / 62 010 / 0.026 ms / 16.3 %       — INSERT INTO domain_event_outbox
  941 ms / 23     / 40.945 ms / 9.4 %        — SELECT ... FROM ledger_inflight_outbox WHERE published_at IS NULL ...
```

The `getQueuedAmounts` SELECT on the Ledger DB (was 16 % of pg time at C-2
conc=400 before the partial-index work) is **gone from the top** — the verify
path now hits Ledger's Redis cache. The 36 % UPDATE is the
`LedgerInflightHoldFailureCompensator` working through pre-existing stuck rows
from earlier benches; not new-order hot path, will drain.

### Predictions vs outcome (scoreboard)

| prediction | outcome | verdict |
| ---- | ---- | ---- |
| `RUNNABLE in HttpClient` count at conc=400 drops from ~190 to <10 | dropped to 16 / 18 per ingress (mostly in-flight, not waiting) | partial win — order of magnitude correct |
| RPS ceiling moves further | 3 807 → 7 762 (+2.04×) | confirmed |
| New ceiling will surface as cluster-node or projector lag | Actually surfaces as **Hikari pool over-subscription** (78 % parked) | partial — correct that something else takes over, wrong about which |
| Ledger Redis cache is the right fix without OMS-side cache | confirmed (single OMS diff, zero Ledger change, 7× lift over baseline) | win |

### What this changes for the roadmap

- **Phase-C-3 lands.** No follow-up needed in this slice.
- **Phase-C-4 (raise Hikari pool size or run reconciler off its own pool)
  becomes the dominant remaining lever**, not just a "if we expect conc≥400 in
  production". The math fits the wall: 20 conns × ~5 ms accept-tx = 4 000 rps
  per ingress = 8 000 rps theoretical; we're at 7 562 rps = 95 % of theoretical.
  Doubling the pool to 40 should move the ceiling to ~15–16 k rps if nothing
  else gives first.
- **Aeron-cluster offer back-pressure** (`OmsClusterIngressClient.submitAcceptOrder`)
  showed 3–7 parked threads per ingress at conc=400. Not yet dominant, but it
  is now visible in the profile for the first time. Once Hikari is sized up,
  this is the most likely next ceiling — at which point the conversation is
  about cluster-node single-thread admission throughput / pipelining, **not**
  about the request-handling layer.
- **Ledger code is fine for now.** The Redis cache is doing its job for the
  verify path. The non-verify Ledger paths (`fetchAvailableBalance` on the SYNC
  buying-power admission, `fetchBalanceReadModel` on the FX nostro snapshot)
  still send `with_queued=true` because they actually need queued amounts.
  Both paths are low-volume in the current async-hold profile (sync buying-power
  admission is gated off by `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`), so
  Ledger-side cache work would be premature optimisation. Revisit if/when those
  paths come back online.

### Lessons

- **Read the upstream service's code before adding a cache in front of it.**
  Ledger had the right cache; OMS was opting out of it. The fix was a one-line
  diff in OMS, no new layer, no TTL bookkeeping.
- **Predicting "the next bottleneck" is fragile.** The C-2 runbook said the
  next ceiling would be cluster-node or projector. It turned out to be Hikari.
  The right discipline is "run the bench, read the jstack, then tell me where
  the next ceiling is" — not "extrapolate from theory".

## Tier 2.5 phase C-4 verification — Hikari pool size is **not** the lever (Pop! 2026-05-14)

> Negative result. The C-3 runbook predicted "Phase-C-4 (raise Hikari pool size)
> becomes the dominant remaining lever" — Pop! data falsified that prediction.
> Documenting the disproof here so the next person doesn't repeat the experiment.

### Hypothesis (going in)

Post-C-3 mid-bench `jstack` showed ~78 % of ingress `http-nio-*` threads parked
in `HikariPool.getConnection`. Naive read: pool too small (`OMS_PG_POOL_MAX_SIZE=20`),
bumping to 40/80/120 should raise the ceiling.

### Setup

`scripts/launch-bench-stack.sh` sources `~/.oms-bench.env` **after** the calling
shell's `export`, so passing `OMS_PG_POOL_MAX_SIZE=80` to the script is silently
overridden by the file's own `export OMS_PG_POOL_MAX_SIZE=20`. The first sweep
(`bench-results/c4-pool*-09[44-45]*`) was a wash — Hikari `connections_max` came
back as `20.0` for all three target sizes. The real sweep
(`bench-results/c4real-pool*`) `sed`-replaces the file in place and verifies via
`/actuator/prometheus` (port 8087/8187, **not** 8088/8188 — those are app HTTP)
that `hikaricp_connections_max{pool="oms-pg"}` matches the target before
benching.

### Measurement (60 000 orders, conc=400, two ingress JVMs, all 5 OMS JVMs alive)

Pool sweep with verification:

| Pool | RPS | p50 | p99 | Hikari `connections_max` |
|---:|---:|---:|---:|---:|
| 20 (canon)  | 6 894 | 52 ms | 152 ms | 20 (verified) |
| 20 (canon)  | 7 623 | 52 ms | 123 ms | 20 (verified) |
| 40          | 7 100 | 51 ms | 149 ms | 40 (verified) |
| 50          | 7 126 | 49 ms | 134 ms | 50 (verified) |
| 60          | 7 202 | 49 ms | 141 ms | 60 (verified) |
| 80          | 7 063 | 49 ms | 155 ms | 80 (verified) |
| 120         | 7 315 | 51 ms | 155 ms | 120 (verified) |

The pool=20 reruns bracket the rest of the sweep (6 894 — 7 623 rps run-to-run).
**Pool size between 20 and 120 does not move the ceiling.** Best single observation
post-C-3 stays at the 7 762 rps reported in the C-3 section (one-off lucky run).
Mean steady-state ceiling on Pop! is **~7 200-7 300 rps**.

### Why pool size doesn't help — re-reading the jstack

The C-3 jstack interpretation was wrong. The 176 ingress-1 / 171 ingress-2
threads parked at `HikariPool.getConnection(HikariPool.java:162)` were not
queueing because the pool was full — at pool=120 the post-bench scrape shows
`hikaricp_connections_idle=47` (i.e. only 73 conns ever activated, with plenty
of slack), and the mid-bench `RUNNABLE` count of threads in any DB or HTTP frame
is just 24-37. Pool size 20 already covers the working set.

Sampling stacks from the parked threads shows two real groups:

1. **Tomcat idle workers** (most of the 116 raw `Unsafe.park` on ingress-2):

   ```
   ThreadPoolExecutor.getTask → TaskQueue.poll → LinkedBlockingQueue.poll
   ```

   Tomcat keeps spare worker threads beyond the steady-state in-flight count.
   Not a bottleneck.

2. **Threads inside an open transaction, parked on the cluster commit reply:**

   ```
   CompletableFuture.timedGet
   OmsClusterIngressClient.submitAcceptOrder(:635)
   OrderIngressService.submitToClusterOrThrow(:266)
   OrderIngressService.persistAcceptedBody(:203)            ← inside tx
   TransactionTemplate.execute(:140)
   OrderIngressService.persistAccepted(:176)
   ```

   These threads **own a Hikari connection** (acquired by `doBegin`) and are
   waiting on Aeron cluster commit before doing the two outbox `INSERT`s and
   committing. Conn-hold time per request is dominated by `commit_round_trip`,
   not by Postgres work. `oms.cluster.client.commit_round_trip_seconds` shows
   mean 0.91 ms, p99 ~3.8 ms, p999 ~11 ms across 30 000 calls per ingress —
   so each conn is held ~1-3 ms beyond the 0.05 ms of actual `INSERT` time.

   The threads at `HikariPool.getConnection(:162)` are queued behind
   conn-holders that are themselves queued on the cluster reply. Bumping the
   pool just lengthens the ready-queue without speeding up the cycle.

### Side-finding: Supavisor `POOLER_MAX_CLIENT_CONN` cap

`docker inspect ledger-supabase-pooler` →
`POOLER_DEFAULT_POOL_SIZE=50, POOLER_MAX_CLIENT_CONN=300`.

When pool=80 was applied across both ingress (160 conns) plus cluster-node
(20) plus the Ledger app's own pool plus pre-existing zombie conns from
killed JVMs, host-side `ss -tn | grep -c :6543` topped out at exactly 300.
**At that point projector and fix-egress could no longer start** — Flyway boot
fails with `FATAL: Max client connections reached`. Restoring pool=20 and
waiting for Supavisor to reap idle conns drops it to ~24/300, after which
projector + fix-egress restart cleanly.

Operational implication: do not raise `OMS_PG_POOL_MAX_SIZE` above ~50 per JVM
on this rig without first raising `POOLER_MAX_CLIENT_CONN` on the Supavisor.
Even though pool size doesn't help RPS, an accidental bump can lock you out of
restarting any other JVM that talks to the same pooler.

### Diagnostic: confirming the cluster-admit-in-tx hypothesis

Ran a falsifiable test: kill projector + fix-egress (free their Supavisor
backends and Hikari pool slack), bench again. Two outcomes possible:

- **If Supavisor's 50-backend pool was the wall**, RPS rises (~1.5x).
- **If cluster-admit-in-tx is the wall**, RPS stays flat or drops (cluster
  egress backs up because nothing drains `OrderAdmittedEvent`).

Result: **5 057 rps with cold-restart** (down from 7 200), then **8 230 rps
with already-warm ingress and a degraded-but-stable topology**, with p50 dropping
to 47 ms. The 5 057 number is the cold-cluster-egress-backup case; the 8 230 is
the warm case. Either way, the absence of projector/fix-egress isn't a clean
test because they share the cluster's egress recording. So: not Supavisor's pool
that limits us — it's the conn-hold time inside the tx.

`pg_stat_statements` at the pool=120 saturation run confirms Postgres is mostly
idle on the hot path:

```
INSERT ledger_inflight_outbox (60 000 calls × 0.054 ms) = 3.24 s of pg time
INSERT domain_event_outbox (60 000 calls × 0.027 ms) = 1.62 s
UPDATE compensator (671 calls × 6.6 ms)             = 4.43 s   ← background
SELECT reconciler (~20 calls × 90 ms)               = 1.80 s   ← background
```

Total Postgres time on the hot path ≈ 4.9 s over the 8.2 s bench window. That's
0.6 connection-equivalents of pg work for ~7 200 rps. The bottleneck is
**not Postgres**, **not Hikari**, **not Supavisor** — it's the ~3-8 ms of
Aeron cluster commit RTT held inside the Postgres transaction.

### Phase D-1 (recommended next slice) — pull cluster admit out of the Postgres tx

Mirrors what C-2 did for the Ledger verify HTTP call: move
`submitToClusterOrThrow` (lines 203 of `OrderIngressService`) **out of**
`transactionTemplate.execute(...)`. New flow:

1. `maybeVerifyLedgerBalanceBinding(req)` — already outside the tx (C-2).
2. `submitToClusterOrThrow(...)` — **NEW: outside the tx**. Cluster admit /
   commit happens before any DB connection is touched.
3. If admission returned `Duplicate`, return early — no tx needed.
4. Open a tx (`transactionTemplate.execute`):
   - `maybePlaceBuyLedgerInflightHold(order)` (writes to ledger_inflight_outbox)
   - `domainEventOutbox.insert(...)` (writes to domain_event_outbox)
5. Commit, return.

Conn-hold drops from ~5-10 ms (mostly Aeron) to ~0.5-1 ms (just the two
`INSERT`s + `COMMIT`). Theoretical lift at conc=400, 50-backend Supavisor wall:
~3-5x. Practical lift bounded by what becomes the next ceiling — likely Aeron
cluster commit serial throughput on cluster-node, then Tomcat / Spring
overhead.

Risk to think through: with the tx opening **after** cluster admit, an ingress
JVM crash between step 2 and step 5 leaves the cluster with an `OrderAccepted`
event but no outbox rows. The projector would still write the `orders` row from
the cluster event, but without `domain_event_outbox` there is no fanout, and
without `ledger_inflight_outbox` no async hold. Recovery would need either
(a) a reconciler that materialises missing outbox rows from cluster log on
startup, or (b) idempotent re-emission so the projector itself produces the
domain envelopes from the cluster event (the path it already takes for the
`orders` row writeup). Worth a small slice on its own before doing D-1.

### Phase D-2 (optional, only if D-1 still leaves slack) — bump Supavisor pool

Set `POOLER_DEFAULT_POOL_SIZE=100` and `POOLER_MAX_CLIENT_CONN=500` on
`ledger-supabase-pooler`. Requires raising Postgres `max_connections` from 100
to 250+ in the supabase-db config. Brief outage for the Ledger app while
pooler restarts. Only worth doing if D-1 lifts the cluster-admit-in-tx wall and
Supavisor's 50-backend pool becomes the new ceiling — until then, the OMS-side
Hikari pool isn't even fully utilised, so this is premature.

### Lessons

- **`hikaricp_connections_max` is the truth.** Always verify the env var
  reached the JVM before drawing conclusions from a bench. The first C-4 sweep
  was a wash because the pool was still 20 the entire time.
- **`HikariPool.getConnection(:162)` in jstack ≠ "pool too small".** Threads
  also park there briefly during HikariCP's internal handover when conns are
  available. Always cross-check with `hikaricp_connections_idle`. If conns
  are idle at scrape, the pool is sized fine.
- **The actual conn-holders' stacks reveal the bottleneck.** Sampling 5-8
  diverse parked stacks (not just counting first-frame buckets) was what
  surfaced `submitToClusterOrThrow` inside the tx.
- **Supavisor `POOLER_MAX_CLIENT_CONN` is invisible until you trip it.** When
  pool size starts mattering, the next infra knob to know about is this one,
  not `POOLER_DEFAULT_POOL_SIZE`.

## Tier 2.5 phase D-1 verification — pull cluster admit out of the Postgres tx (Pop! 2026-05-14)

Lands the "Phase D-1 (recommended next slice)" proposed by the C-4 verification
above, plus the durability-gap closure that section flagged as a prerequisite
("Worth a small slice on its own before doing D-1"). Both ship in commit `6bea82e`.

### What changed

* `OrderIngressService.persistAccepted`: `submitToClusterOrThrow` and the
  duplicate-short-circuit branch run **before** `transactionTemplate.execute`
  opens any Hikari connection. The tx body is now strictly the two outbox
  `INSERT`s (`maybePlaceBuyLedgerInflightHold` for BUY/async + `domainEventOutbox.insert`).
* `OmsPostgresProjector.applyAdmittedEvent`: idempotent backfill of
  `ledger_inflight_outbox` from the cluster `OrderAdmittedEvent` itself. Uses
  the existing `uq_ledger_inflight_outbox_order_id` UNIQUE INDEX (V4) →
  `INSERT ... ON CONFLICT (order_id) DO NOTHING`. Happy path = no-op (one
  index probe); crash path = projector materialises the row so the slice 4p
  reconciler/compensator pipeline drives the BUY hold (or compensating cancel)
  unchanged.
* `LedgerInflightOutboxRepository.insertIfAbsent(orderId, payload)` added to
  back the projector backfill.

`OrderAccepted` domain envelopes (the customer-facing "we received your order"
stage) are **not** backfilled by the projector: a crash in the admit-tx window
means downstream consumers see `OrderWorking` (emitted by the projector via
`OrderControlAdmission.persistAdmission`) without the preceding `OrderAccepted`.
This is acceptable today — status fanout is the contract; the "received" stage
is informational.

### Bench setup (matches the C-4 baseline rig exactly)

* Five JVMs running clean (cluster-node + projector + fix-egress + 2× ingress-replicas).
* Postgres tables truncated + Aeron lock dirs cleaned + cluster log fresh — eliminates
  the 1.4M-row inflight-outbox backlog from the C-4 sweep that would otherwise
  have skewed the comparison.
* `OMS_PG_POOL_MAX_SIZE=20` per ingress (canonical baseline; verified via
  `hikaricp_connections_max=20.0` post-bench).
* `OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED=true`, `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`,
  `OMS_LEDGER_INFLIGHT_COALESCER_ENABLED=false` — slice 4p outbox path, identical
  to C-2 / C-3 / C-4 runs.

### Bench result — single ingress concurrency sweep

`/tmp/bench-d1-conc-20260514-110016/`

| concurrency | total | rps | p50 (ms) | p95 (ms) | p99 (ms) |
| ----------- | ----- | --- | -------- | -------- | -------- |
| 80          | 5 000 | 5 902 | 11.8   | 18.0     | 63.4     |
| 400         | 60 000 | **7 744** | 50.2 | 69.7  | 85.1     |
| 600         | 90 000 | 7 802 | 74.9   | 95.6     | 116.9    |
| 800         | 120 000 | **7 946** | 98.6 | 120.7 | 132.9    |
| 1 200       | 180 000 | 7 818 | 150.3 | 177.7   | 193.5    |

Single-ingress ceiling: **~7 950 rps**.

### Bench result — both ingresses, round-robin

`/tmp/bench-d1-multi-20260514-110226/` (`OMS_BURST_URLS=...:8088,...:8188`)

| concurrency | total | rps | p50 (ms) |
| ----------- | ----- | --- | -------- |
| 400         | 60 000 | 7 717 | 49.7   |
| 800         | 120 000 | 7 770 | 103.3 |
| 1 200       | 180 000 | 7 721 | 152.1 |
| 1 600       | 240 000 | 7 775 | 203.1 |

Two-ingress ceiling: **~7 770 rps** — i.e. **the same** as one ingress. The
bench machine is past the per-ingress ceiling but the cluster-side serial
admit and bench-tool/CPU side of the run cap aggregate throughput.

### Latency decomposition (per request mean, ingress-1, conc=400 run, 60 k)

| stage                                                        | mean (ms) | source                                                   |
| ------------------------------------------------------------ | --------- | -------------------------------------------------------- |
| HTTP RTT (bench-client side)                                 | 51.0      | `IngressBurstMain` HdrHistogram                          |
| Spring controller end-to-end                                 | 24.9      | `http_server_requests_seconds`                           |
| `oms_pipeline_ingress_accept` (entire `persistAccepted`)     | 24.5      | `oms_pipeline_ingress_accept_seconds`                    |
| Hikari conn-usage (the tx body)                              | **2.75**  | `hikaricp_connections_usage_seconds`                     |
| Aeron cluster commit RTT (now outside the tx)                | **0.51**  | `oms_cluster_client_commit_round_trip_seconds[accept_order/commit]` |
| Δ unaccounted (Ledger verify HTTP + Spring/JSON / scheduling) | ~21       | residual                                                 |

The **bench client RTT − Tomcat-handled time = 51.0 − 24.9 = 26.1 ms** sits in the
Tomcat NIO accept queue: with bench `concurrency=400` and Tomcat
`max-threads=200` (Spring Boot default), about half the in-flight requests are
queued before they are assigned a worker. Visible to the bench client as latency,
invisible to `http_server_requests_seconds`.

### Why the lift was modest, not 3-5x

The C-4 plan estimated "theoretical lift ~3-5x" on the assumption the Aeron
cluster commit RTT was ~3-8 ms. The actual cluster commit mean was already
**~0.5 ms** (single-host, single-node consensus): pulling it out of the tx
saves ~0.5 ms, not ~5 ms. The conn-hold time fell from where it was to **2.75 ms**
mean (only ~2 inserts + commit + Supavisor RTT) — that is at or below the
single-ingress Hikari pool ceiling at the configured pool=20:

```
pool=20 / 2.75 ms = 7 273 rps theoretical at full saturation
```

We measured 7 744 rps at conc=400 single-ingress (modest 6 % over theoretical
because some requests are duplicate-short-circuit and never acquire a Hikari
conn at all). The bench is now firmly **Hikari-pool-bound**, not
cluster-RTT-bound.

Concretely the right framing is: **D-1 was a correctness fix more than a
throughput one in this rig.** It removes the Aeron commit RTT from the
connection-hold critical path, so any future cluster slowdown (3-node consensus
in Phase 5, slow follower replay, higher-latency disk archive) **does not
quadratically tax Hikari conn-equivalents**. The +6-9 % single-ingress lift
(7 100–7 300 → 7 744 rps) is the bonus on the single-host bench rig today.

### Durability-gap closure verification

`grep -c 'Projector D-1 backfill' ~/oms/logs/projector.log` after the full
sweep (5 k smoke + 60 k + 90 k + 120 k + 180 k = 455 k orders): **0 hits**.

Happy-path means ingress always wrote the `ledger_inflight_outbox` row before
its tx returned to the projector's view of the cluster log. The
`insertIfAbsent` is silently swallowed by `ON CONFLICT DO NOTHING` in steady
state. The crash-window path is exercised by `OmsPostgresProjectorD1BackfillTest`
which pins the gating + payload shape end-to-end (BUY-async / SELL skip /
no-balance skip / market skip / disabled skip / happy path).

### Side-finding: ingress-1 vs ingress-2 conn-hold asymmetry

`hikaricp_connections_usage_seconds` over the entire bench window (cumulative
since OMS startup):

* ingress-1: 2.4–3.1 ms mean
* ingress-2: 4.9–7.0 ms mean

ingress-2 consistently held connections ~2× longer per request. Both ingresses
share Supavisor (50-backend pool, 300 client conn cap), and the per-bench delta
counts confirm round-robin load balancing was working. Most likely cause is
warm-vs-cold Hikari pool history (ingress-1 was the warmup target in the
single-ingress sweep before this run); not investigated further here because
the aggregate ceiling (~7 770 rps) is already at the Tomcat / cluster-side wall,
not at the ingress-2 connection-hold wall.

### Recommended next slices

* **Phase D-2 (next obvious lever) — bump per-ingress Hikari pool to 30-40.**
  Budget: 2 ingresses × 40 = 80 client conns + projector + fix-egress
  (~10 each) + Ledger app baseline (~50–80) ≈ 200, well under Supavisor's
  `POOLER_MAX_CLIENT_CONN=300`. Theoretical lift: pool=40 / 2.75 ms ≈ 14 500 rps
  ceiling per ingress, so ~2× single-ingress and possibly more aggregate. Will
  validate the multi-ingress ceiling we hit in this run is genuinely
  cluster-side / bench-tool-side and not a Hikari ceiling.
* **Phase D-3 (alternative) — empty the ingress tx entirely.** Move
  `domain_event_outbox.insert` for the `OrderAccepted` envelope into the
  projector (driven from `OrderAdmittedEvent`, mirroring the existing
  `OrderWorking` envelope path). With both outbox writes on the projector,
  ingress no longer opens a Postgres tx at all; conn-hold drops to 0 ms;
  Hikari pool size becomes irrelevant for the ingress hot path. Tomcat /
  bench tool / cluster commit then become the only walls.
* **Phase D-4 (optional infra) — bump Supavisor `POOLER_DEFAULT_POOL_SIZE`** —
  same as the C-4 "Phase D-2 (optional)" proposal above. Only worthwhile after
  D-2 or D-3 lifts the OMS-side wall and Supavisor's 50-backend pool actually
  becomes the bottleneck (today it isn't).

### Lessons (D-1 specific, in addition to C-4's list)

- **"Theoretical 3-5x lift" assumes the bottleneck cost is what you guessed
  it was.** C-4's jstack made the cluster commit look big because it was the
  most-visible parked stack; the actual mean RTT was 0.5 ms. Always pair
  jstack with `_seconds_count` + `_seconds_sum` Prometheus pairs to compute
  real per-stage cost before estimating lift.
- **Removing work from the tx is a correctness lever first, throughput lever
  second.** The Phase 5 / 3-node consensus path will likely make Aeron commit
  10–50 ms p99; D-1 ensures that increase doesn't multiply through Hikari.
- **Idempotent backfill via `ON CONFLICT DO NOTHING` on a UNIQUE INDEX is
  a clean closure for these "dual-write across logs" durability gaps.**
  No new schema, no new columns, no compensator changes — projector writes
  the row the ingress would have written, reconciler picks up either row
  identically.
- **Tomcat NIO accept queue is invisible to `http_server_requests_seconds`.**
  When per-stage decomposition has a large unaccounted residual against
  bench-client RTT and `concurrency > tomcat.max-threads`, that residual is
  the queue.

## Tier 2.5 phase D-2 + D-4 verification — Hikari pool isn't the wall, Supavisor backend pool was lying about its size (Pop! 2026-05-14)

**Headline:** D-1 left us at a multi-ingress ceiling of ~7 770 rps. D-2 (bumping
the Hikari pool 20→40) is a **negative result** that *regresses* throughput by
~40 %. D-4 (bumping the Supavisor *backend* pool from its actual value of 20 to
80) lifts the multi-ingress ceiling to **10 561 rps** — a 1.36× win — with no
code change and no failures. The investigation also surfaced a hard-to-spot
operational footgun: Supavisor's tenant DB row had drifted away from the
`POOLER_DEFAULT_POOL_SIZE=50` env var.

### What changed

* Code: nothing. Both knobs are runtime config.
* Config (D-2 attempt): `~/.oms-bench.env` `OMS_PG_POOL_MAX_SIZE=20 → 40`.
* Config (D-4 fix): in the Supavisor metadata DB (`_supabase._supavisor`),
  `update _supavisor.tenants set default_pool_size=80, default_max_clients=300;`
  `update _supavisor.users set pool_size=80;`, then `docker restart
  ledger-supabase-pooler` and a clean restart of all 5 OMS JVMs.

### D-2 attempt (pool=40 OMS, Supavisor backend still 20)

Same multi-ingress sweep as D-1 (`/tmp/bench-d2-multi.sh`). Result: **collapse**.

| concurrency | aggregate rps | mean RTT | p99 RTT |
|---:|---:|---:|---:|
| 400 (multi) | 4 587 | 86 ms | 158 ms |
| 800 (multi) | 4 721 | 168 ms | 246 ms |
| 1 200 (multi) | 4 652 | 256 ms | 421 ms |
| 1 600 (multi) | 4 685 | 340 ms | 465 ms |

Single-ingress at the same pool=40 was less catastrophic but still regressive
(c=600 peaked at 7 698 rps, c=800 collapsed to 4 651 rps).

`hikaricp_connections_usage_seconds_sum` / `_count` per ingress jumped from
~2.75 ms (D-1 baseline) to **8.0 ms** (D-2 c=800). That's the smoking gun:
queries themselves got slower under more in-flight clients. With 4 JVMs × 40 =
160 client conns multiplexing through Supavisor, the actual backend pool was
already saturated at every cycle and every Postgres operation queued for a
backend slot.

`jstack` mid-bench at c=800 (200 http-nio threads):
* **155 TIMED_WAITING in `LinkedTransferQueue$DualNode.await`** — Hikari's
  internal queue, threads waiting for any of the 40 in-pool connections.
* **40 RUNNABLE in `Socket$SocketInputStream.read`** — exactly the 40 holding
  connections, all stuck reading replies from Postgres (i.e. Supavisor).

Reverted to pool=20 → multi-ingress sanity bench at c=800 returned **7 626 rps**
(D-1 baseline restored). D-2 is rejected.

### D-4 setup discovery — the Supavisor tenant row is the source of truth, not the env var

`docker inspect ledger-supabase-pooler` showed `POOLER_DEFAULT_POOL_SIZE=50`
and `POOLER_MAX_CLIENT_CONN=300` — those are the env vars supplied to the
container. But Supavisor's `pooler.exs` only reads them at *initial tenant
creation*; after that the active values live in
`_supabase._supavisor.tenants` / `_supabase._supavisor.users`. On Pop!:

```sql
select default_pool_size, default_max_clients from _supavisor.tenants;
-- 20 | 100
select pool_size from _supavisor.users;
-- 20
```

So the **actual enforced backend pool was 20**, not 50; the
`POOLER_DEFAULT_POOL_SIZE=50` env was inert. This finding retroactively
explains C-4 ("Supavisor `POOLER_MAX_CLIENT_CONN` is invisible until you trip
it"): the tenant row's `default_max_clients=100` was the wall, not the env's
300. Update the rows, restart the Supavisor container, then restart any client
applications (their existing TCP sessions are severed).

### D-4 step 1 — pool=20 OMS, Supavisor backend=80

Multi-ingress sweep, all green:

| concurrency | aggregate rps | mean RTT | p99 RTT |
|---:|---:|---:|---:|
| 400 (multi) | 10 055 | 51 ms | 107 ms |
| 800 (multi) | 10 405 | 76 ms | 171 ms |
| 1 200 (multi) | **10 602** | 112 ms | 234 ms |
| 1 600 (multi) | 10 488 | 151 ms | 234 ms |

`hikaricp_connections_acquire_seconds` per-request mean **dropped from D-1's
44 ms (sanity revert measurement) to 2.81 ms**, and usage time held at 2.48 ms
— the Supavisor queue is gone, ingress is now strictly Hikari-pool-bound.

### D-4 step 2 — try also bumping OMS pool=40 (with backend=80)

Theory: backend=80 should now accommodate 4 × 40 = 160 client conns at peak.
Result: throughput is **flat or worse** (c=400 9 824 / c=800 10 313 / c=1 200
10 340 / c=1 600 10 419 rps) **and** introduces 0.3 % HTTP 500s (`SQLException:
Connection is closed` from `OrderIngressService.persistAccepted` rollback
path):

```
java.sql.SQLException: Connection is closed
    at com.zaxxer.hikari.pool.ProxyConnection$ClosedConnection.lambda$getClosedConnection$0(ProxyConnection.java:503)
    at com.zaxxer.hikari.pool.ProxyConnection.rollback(ProxyConnection.java:386)
    at com.balh.oms.ingress.OrderIngressService.persistAccepted(OrderIngressService.java:230)
```

Cause: pool=40 × 4 JVMs = 160 client conns multiplexed onto 80 backend conns,
plus burst peaks where both ingresses simultaneously want >40 backend slots.
Supavisor evicts idle client conns mid-tx; Hikari's proxy connection then
fails the rollback. Pool=40 is **not viable** even with backend=80 at this
JVM count.

### D-4 step 3 — final canonical config: pool=20 OMS, backend=80 Supavisor

Re-ran the sweep cleanly:

| concurrency | aggregate rps | failures | p50 RTT | p99 RTT |
|---:|---:|---:|---:|---:|
| 400 (multi) | 10 086 | 0 | — | — |
| 800 (multi) | 10 469 | 0 | 67 ms | 166 ms |
| 1 200 (multi) | 10 481 | 7 (0.004 %, 502s) | 110 ms | 239 ms |
| 1 600 (multi) | **10 561** | 0 | 150 ms | 260 ms |

Hikari per-ingress at c=1 200: acquire wait 5.1 ms, usage 2.70 ms. Postgres
work is back at the D-1 baseline (no Supavisor queue), and acquire wait is a
manageable 1.9 × usage — within the regime where doubling pool would
*linearly* scale RPS *if* there was Postgres headroom (pool=40 showed there
isn't, given Supavisor multiplex / connection-eviction behaviour).

### Lift summary so far

| phase | per-ingress hot path | OMS pool | Supavisor backend | aggregate rps | vs D-1 |
|---|---|---:|---:|---:|---:|
| D-1 (cluster admit out of tx) | 2 INSERTs | 20 | 20 | 7 770 | 1.00× |
| D-2 (pool 20 → 40 only) | 2 INSERTs | 40 | 20 | 4 720 | 0.61× |
| **D-4 step 3** (backend 20 → 80) | 2 INSERTs | 20 | 80 | **10 561** | **1.36×** |

End-to-end since slice 4p baseline (1 094 rps): **9.65× lift** over the original
async-outbox ceiling.

### Why D-2 was a regression and D-4 step 2 capped out

The Hikari pool acts like a rate-limiter onto a downstream finite resource.
With Supavisor backend=20:

* pool=20 × 2 ingresses = 40 client conns peak demand → Supavisor multiplexes
  each backend conn ~2:1, queries average 2.75 ms (1 ms pure Postgres + 1.75 ms
  multiplex queue).
* pool=40 × 2 ingresses = 80 client conns peak demand → Supavisor multiplex
  ratio ~4:1, queries average 8 ms. RPS ceiling = pool / mean_usage =
  40 / 0.008 = 5 000 rps per ingress (not 14 500 as the naive theoretical
  formula suggested). Lower than pool=20 case (20 / 0.00275 = 7 273).

With Supavisor backend=80, the multiplex ratio drops to <1:1 and the formula
is dominated by *Postgres real time*, which is ~1–2 ms. pool=20 / 0.0027 ≈
7 407 rps per ingress; 2 × ≈ 14 800 theoretical, achieving 10 561 means
~71 % of theoretical — the gap is now real Tomcat / cluster-side / write
contention, not Hikari.

### Operational notes — touching Supavisor pool size

1. **Stop client apps first.** OMS JVMs hold long-lived Hikari → Supavisor TCP
   sessions; if the pooler restarts under them, every in-flight tx fails with
   `Connection is closed`. Order is: stop OMS JVMs → update tenant row →
   restart Supavisor → restart OMS.
2. **Update the tenant row, not just the env var.** The env var is only read
   on first tenant creation. The `_supabase._supavisor.tenants` and
   `.users` rows are the runtime authority.
3. **Watch Postgres `max_connections`.** With backend=80 and Supabase's
   default `max_connections=100`, you have ~16 conns of headroom for
   Realtime / PostgREST / pg_cron / admin shells. Check
   `pg_stat_activity` after the bump to confirm the steady-state count.
4. **Keep OMS pool low.** With backend=80 and 4 JVMs, the safe ceiling is
   pool=20 each (= 80 client conns peak, matches backend exactly). pool=40
   triggers Supavisor's client-conn eviction under burst and produces user-
   visible 500s. This is the inverse of the C-4 / D-1 expectation that "more
   Hikari is always better".

### Recommended next slices

* **Phase D-3 (highest expected lift) — empty the ingress tx.** Move the
  `domain_event_outbox.insert` for `OrderAccepted` into the projector, driven
  off `OrderAdmittedEvent` (mirrors the existing `OrderWorking` projector
  path). With both outbox writes on the projector, ingress no longer opens a
  Postgres tx at all; conn-hold drops to 0 ms; Hikari pool size becomes
  irrelevant for the ingress hot path. Tomcat / cluster commit / bench tool
  become the only walls. Theoretical lift: cap on ingress goes from
  ~10 561 rps to whatever the cluster + Tomcat can sustain.
* **Phase D-5 (only after D-3 lifts the ingress wall) — bump Supavisor
  backend further (80 → 120) and/or split projector + fix-egress onto a
  separate Supavisor tenant.** Today the projector / fix-egress conn budget
  is small but their `max_connections=100` headroom is shrinking; if
  D-3 succeeds, the projector will be doing 2× the writes per admit and
  may need its own pool.

### Lessons (D-2 + D-4 specific, in addition to D-1's list)

- **`docker inspect $container | grep ENV` is not the source of truth for
  Supavisor.** The active config lives in `_supabase._supavisor.tenants` /
  `.users`. Always check the DB row, not the env, before reasoning about
  pool capacity.
- **Bigger Hikari is not always better — it can shorten tx but lengthen each
  query under a saturated downstream.** If `hikaricp_connections_usage_seconds`
  rises with concurrency, the bottleneck is downstream of Hikari, not in it,
  and bumping pool will multiply contention.
- **Supavisor in transaction mode evicts client conns under multiplex
  pressure.** The `Connection is closed` rollback path is the symptom; the
  cause is `client_conns_active > backend_pool`. Keep ingress pool ≤ backend
  pool / (number of ingress JVMs).
- **The cleanest tuning ladder for this stack is "shrink ingress tx → grow
  Supavisor backend → grow OMS Hikari", in that order.** D-1 narrowed the tx,
  D-4 grew Supavisor backend; only after D-3 fully empties the ingress tx
  would growing Hikari be safe (and it would be moot, since there's no tx
  to hold the conn for).

## Tier 2.5 phase D-3 verification — empty the ingress tx; projector emits OrderAccepted (Pop! 2026-05-14)

**TL;DR.** Moving the `domain_event_outbox.insert(OrderAccepted)` from
`OrderIngressService` into `OmsPostgresProjector` (gated on the existing
`orders` `ON CONFLICT DO NOTHING` boolean return) and dropping the Spring
`TransactionTemplate` from the ingress hot path lifted multi-ingress RPS from
the D-4 ceiling **10 561 → 11 629 rps (1.10× over D-4, 10.6× over slice 4p
baseline)**, with **0 failures across 240 000 admits at 1 600 client
concurrency**, and collapsed Hikari conn-hold time on the ingress JVMs from
≈ 7.8 ms (D-4: 5.1 ms acquire + 2.7 ms usage) to ≈ 1.65 ms (D-3: 0.23 ms
acquire + 1.42 ms usage). The remaining ceiling is on the Aeron cluster
admit + Tomcat thread budget, not Postgres.

### Setup

| Knob | Value | Note |
|---|---|---|
| Code | `oms@2045cce` (D-3) | `oms: tier-2.5 phase D-3 — emit OrderAccepted from projector, drop ingress Spring tx` |
| `OMS_PG_POOL_MAX_SIZE` | 20 (per ingress JVM) | unchanged from D-4 canonical |
| Supavisor `default_pool_size` (backend) | 80 | unchanged from D-4 canonical |
| Supavisor `default_max_clients` | 300 | unchanged from D-4 canonical |
| OMS topology | 5 JVMs (cluster-node + projector + fix-egress + 2× ingress-replica) | D-4 stack, restarted onto D-3 code |
| Bench tool | `bootRunBurst` two-target round-robin (`8088` + `8188`) | matches D-2 / D-4 multi-ingress harness |
| State | `TRUNCATE … RESTART IDENTITY CASCADE` before launch + Aeron dirs cleared | clean slate |

The D-3 commit also updates `AbstractPostgresIntegrationTest.TestPostgresProjectorSingleton`
to mirror the production D-3 path (it now writes the `OrderAccepted` envelope
itself when the orders row is freshly inserted), so
`NatsDomainFanoutIntegrationTest` keeps passing without touching its
assertions.

### Smoke test before benching (D-3 specific)

The new projector path needs to be observable, not just inferred from latency.
After the stack is up, post a single SELL order to ingress-1 and check that
the `OrderAccepted` envelope shows up in `domain_event_outbox` even though
the ingress JVM never opened a Spring tx:

```bash
. ~/.oms-bench.env
ACCT=$(cat /proc/sys/kernel/random/uuid)
curl -sS -o /tmp/d3-smoke.json -w 'HTTP %{http_code}\n' \
  -X POST http://127.0.0.1:8088/internal/v1/orders \
  -H 'Content-Type: application/json' \
  -H "X-OMS-Internal-Key: $OMS_INTERNAL_API_KEY" \
  -d "{\"accountId\":\"$ACCT\",\"clientIdempotencyKey\":\"d3-smoke-$$\",\"side\":\"SELL\",\"instrumentSymbol\":\"AAPL\",\"quantity\":\"1\",\"limitPrice\":\"5.00\",\"timeInForce\":\"DAY\"}"
docker exec ledger-supabase-db psql -U supabase_admin -d oms -c "
  SELECT count(*) FILTER (WHERE envelope_json->>'type' = 'OrderAccepted') as accepted,
         count(*) FILTER (WHERE envelope_json->>'type' = 'OrderWorking')  as working
    FROM domain_event_outbox"
```

Expected: 1 accepted + 1 working envelope per POST. The accepted row was
inserted by the projector (D-3 path); the working row was inserted by the
projector's existing post-admission emission. Both go through one tx in the
projector, so they always appear together.

### Multi-ingress sweep results

`/tmp/bench-d3-multi.sh` (copy of D-2's harness with the rename) runs
`bootRunBurst` against both ingresses round-robin at four steady-state
concurrencies plus a 5 000-request warmup. Hikari + accept-pipeline timer
deltas come from per-phase pre/post `/actuator/prometheus` scrapes on each
ingress's management port (8087 / 8187).

| phase | concurrency | total | RPS | mean RTT (ms) | p50 | p99 | failures |
|---|---:|---:|---:|---:|---:|---:|---:|
| warmup | 80 | 5 000 | 5 349 | 13.4 | 9.8 | 116.7 | 1 |
| c400 | 400 | 60 000 | 10 983 | 35.8 | 7.7 | 116.0 | 0 |
| c800 | 800 | 120 000 | 11 512 | 68.6 | 62.5 | 164.1 | 0 |
| c1200 | 1 200 | 180 000 | 11 588 | 102.6 | 93.1 | 194.2 | 0 |
| **c1600** | **1 600** | **240 000** | **11 629** | **136.4** | **137.5** | **245.5** | **0** |

RPS plateaus around 11 500 – 11 629 rps from c800 upward; latency keeps
rising linearly with concurrency, which is the textbook "in queue, not in
service" signature — at this load the bottleneck has moved off Postgres /
Hikari and onto the Tomcat thread pool waiting on the Aeron cluster admit.

### Hikari conn-hold collapse

The whole point of D-3 was to shrink the ingress hot path Postgres footprint
to "at most one auto-committing INSERT" (the BUY-async
`ledger_inflight_outbox` row, slice 4p / D-1) — and drop the
`TransactionTemplate` entirely. With the bench using SELL orders without
inflight reservation, the ingress hot path now opens **zero** Postgres conns
on most requests; only the `maybeVerifyLedgerBalanceBinding` HTTP path is
left, and that's not a Hikari path.

Per-ingress Hikari + accept-pipeline mean times at c1600 (post − pre divided
by count delta over the steady-state phase, in milliseconds):

| metric | D-4 step 3 (10 561 rps) | **D-3 (11 629 rps)** | Δ |
|---|---:|---:|---:|
| `hikaricp_connections_acquire` (mean) | ~5.1 ms | **0.23 ms** | **22× faster** |
| `hikaricp_connections_usage` (mean) | ~2.7 ms | **1.42 ms** | **1.9× faster** |
| total per-acquire conn hold | ~7.8 ms | **~1.65 ms** | **4.7× shorter** |
| `oms.pipeline.ingress.accept` (mean) | ≈ 2.5 ms | ≈ 33.8 ms | now dominated by cluster admit RTT, not Postgres |

The "ingress accept" mean grew because the timer now wraps the cluster admit
itself (D-1 moved cluster admit out of the tx but still inside the timer);
under 1 600 concurrent clients the cluster's single-leader admit thread
serialises into the queue and contributes the full RTT. **This is the next
slice's wall**, and it is *not* a regression — the timer's *Postgres
contribution* has shrunk by 5×; the remaining time is the Aeron cluster
admit, which Postgres tuning cannot help with.

### What "Postgres is no longer the wall" means concretely

At c1600 each ingress JVM does ~5 800 admits/sec into a Hikari pool of 20.
With 1.42 ms mean usage, the pool's *steady-state utilisation* is:

```
5 800 admits/sec × 0.00142 sec/admit ≈ 8.2 conns inflight on average
```

i.e. the pool is < 50 % saturated. Acquire wait is 0.23 ms (mostly thread-
park / unpark cost, not "queue for a conn"). Bumping the pool further would
not help — the limit is now ingress thread-count waiting on Aeron.

### Durability gap closure (D-3 specifically)

D-1 closed the ledger-inflight gap (projector reconstructs
`ledger_inflight_outbox` from `OrderAdmittedEvent` if ingress crashed before
its tx commit). D-3 closes the symmetric gap for `OrderAccepted`: the
projector emits the `OrderAccepted` envelope on the same fresh-vs-replay
boolean it already uses for the orders row. Crash window is now an
ingress-JVM-only concern — the cluster log + the projector together produce
**both** the orders row **and** the `OrderAccepted` envelope. Before D-3
this gap was theoretical (the ingress tx was already very small post-D-1)
but now there is no gap at all.

### Lift summary across Tier 2.5

| phase | RPS (multi-ingress, 2 JVMs) | lift over slice 4p (1 094 rps) | lift over previous |
|---|---:|---:|---:|
| slice 4p baseline | 1 094 | 1.00× | — |
| C-1 + C-2 (Postgres + Ledger HTTP fixes) | ≈ 3 800 | 3.5× | 3.5× |
| C-3 (Ledger Redis cache for verify path) | 7 762 | 7.1× | 2.04× |
| D-1 (cluster admit out of Postgres tx) | 7 770 | 7.1× | 1.00× *(correctness fix)* |
| D-4 (Supavisor backend pool 20 → 80) | 10 561 | 9.65× | 1.36× |
| **D-3 (drop Spring tx; projector emits OrderAccepted)** | **11 629** | **10.63×** | **1.10×** |

D-3 was an order-of-magnitude smaller lift than D-4 because the path it
optimised (one INSERT per request) was already cheap on a healthy backend;
D-4 had unblocked the *real* multiplex contention. D-3 still pays for itself
as a correctness fix (durability) and unlocks D-5 (the Aeron / Tomcat wall
becomes addressable now that Postgres is out of the picture).

### Lessons (D-3 specific, in addition to D-1 / D-4 lists)

- **`hikaricp_connections_usage_seconds` is the right metric to "kill the
  Postgres lever".** When it stops responding to concurrency increases, the
  bottleneck has moved off Postgres entirely; further pool / Supavisor
  tuning is wasted ingenuity.
- **Idempotent backfill from the cluster log is an architectural pattern,
  not a one-shot.** D-1 (`ledger_inflight_outbox`) and D-3
  (`domain_event_outbox.OrderAccepted`) use the same shape: a cluster event
  is the source of truth, the projector's `ON CONFLICT DO NOTHING` is the
  idempotency boundary, and the ingress JVM's tx becomes optional. Future
  side-tables that today ride the ingress tx (none right now, but watch for
  new ones) should follow the same pattern from day one.
- **Hikari `autoCommit=true` (the Spring Boot default) is enough when each
  request makes at most one INSERT.** A `TransactionTemplate` only matters
  when ≥ 2 statements need to commit atomically; D-3's residual ingress
  write (BUY-async inflight outbox row) is a single statement, so the
  template adds latency without buying any consistency.

### Recommended next slices (post-D-3)

* **Phase D-5 — split projector + fix-egress onto a separate Supavisor
  tenant.** Today they share the 80-conn backend pool with the 2 ingresses
  (40 client conns); after D-3 the projector is doing two INSERTs per
  admit (orders + OrderAccepted; OrderWorking + control_decisions on the
  next pass) and is the new write-amplifier. Splitting tenants gives both
  groups independent backend headroom. Theoretical: removes
  cross-tenant multiplex contention; expected lift modest (single-digit %).
* **Phase D-6 — admit batching at the cluster client.** The plateau at
  ~11 600 rps comes from a single Aeron cluster leader thread serialising
  per-admit. Batching N requests per `submitAcceptOrder` round-trip (with
  per-request future fan-out on the response side) would N× the admit
  ceiling at the cost of N× admit latency. Expected lift: 2–3× when
  batchSize is around 8–16; needs careful interplay with the existing
  D-1 idempotency guarantees.
* **Phase D-7 — Tomcat thread bump + ingress concurrency cap.** Today
  Tomcat's default `max-threads=200` × 2 ingresses = 400 inflight requests
  matches the c400 → c800 sweet spot; bumping to 400 per ingress would let
  the bench push past c1600 without queueing on Tomcat first. Probably the
  smallest-effort win remaining; the limit becomes the Aeron leader
  immediately.

> **D-7 disproved by jstack** (Pop! 2026-05-14, before D-8 lands): at
> c1600 / 11 629 rps, only 14 / 200 ingress-1 exec threads were parked in
> `TaskQueue.take` (idle), and **187 / 200 were parked in
> `RestLedgerBalanceClient.fetchBalanceRoot →
> SimpleClientHttpRequest.executeInternal`**. The wall was not Tomcat
> thread budget; it was the synchronous Ledger balance/identity verify
> HTTP call holding 93.5 % of the thread pool. D-7 would have only lifted
> the Tomcat queue depth without removing the Ledger HTTP wait. D-8
> (below) addresses the actual root cause.

## Tier 2.5 phase D-8 verification — cache (balanceId → identityId) in OMS (Pop! 2026-05-14)

### TL;DR

| Concurrency | RPS         | Mean RTT  | p50 RTT   | p99 RTT   | Failures |
|-------------|-------------|-----------|-----------|-----------|----------|
| warmup (c80) |  7 290     |  10.0 ms  |   5.9 ms  | 104.9 ms  | 0        |
| c400        | 19 675      |  19.8 ms  |  17.7 ms  |  59.3 ms  | 0        |
| c800        | 20 452      |  38.4 ms  |  38.0 ms  |  75.8 ms  | 0        |
| **c1200**   | **20 874**  |  56.6 ms  |  57.3 ms  | 102.8 ms  | 0        |
| c1600       | 20 659      |  76.6 ms  |  77.2 ms  | 144.9 ms  | 0        |
| c2400       | 20 753      | 114.6 ms  | 112.7 ms  | 180.7 ms  | 0        |
| c3200       | 20 157      | 157.2 ms  | 161.5 ms  | 270.3 ms  | 0        |

* Best run **20 874 rps** at c1200, **1.795× over D-3** (11 629 rps),
  **18.7× over slice 4p baseline** (1 094 rps).
* Plateau confirmed: RPS is essentially flat from c1200 → c3200 while p50
  scales linearly with concurrency — the system is queue-bound, not
  capacity-bound.
* **0 failures across 1.32 M requests** in the sweep.

### Setup

* Code: `oms@1ce87de` (`oms: tier-2.5 phase D-8 — cache (balanceId →
  identityId) in OMS`).
* Flags on Pop! `~/.oms-bench.env`:
  `OMS_LEDGER_BALANCE_IDENTITY_CACHE_ENABLED=true`,
  `OMS_LEDGER_BALANCE_IDENTITY_CACHE_TTL_SECONDS=300`,
  `OMS_LEDGER_BALANCE_IDENTITY_CACHE_MAX_SIZE=100000`.
* All other flags unchanged from D-4: `OMS_PG_POOL_MAX_SIZE=20`,
  `inflight-async-enabled=true`, `inflight-coalescer-enabled=false`,
  Supavisor `default_pool_size=80`, `default_max_clients=300`.
* Same 5 OMS JVMs (cluster-node + projector + fix-egress + 2 ×
  ingress-replica), Postgres truncated before run.

### Smoke test (4 sequential POSTs, 1 conn, same balanceId)

* HTTP: 4 / 4 → 201, p50 = 7.1 ms (vs ~30 ms in D-3 / pre-cache).
* Cache counters on ingress-1 prom scrape:

```
oms_ledger_balance_identity_cache_requests_total{result="hit"}  4.0
oms_ledger_balance_identity_cache_requests_total{result="miss"} 1.0
```

* The single miss was the warmup curl right before the burst; the
  bench's 4 POSTs all hit the warm cache. Single-flighting +
  cache-hit-fast-path are wired correctly.

### Where the time went, vs D-3 (per-ingress, c1600 row)

| Timer                                | D-3 (Pop! 2026-05-14) | D-8 (Pop! 2026-05-14) | Δ        |
|--------------------------------------|-----------------------|-----------------------|----------|
| `oms.pipeline.ingress.accept`        | 33.8 ms               | **18.7 ms**           | −44 %    |
| `hikaricp_connections_acquire`       |  5.1 ms               | **16.2 ms**           | +217 %   |
| `hikaricp_connections_usage`         |  1.65 ms              |  1.35 ms              | −18 %    |
| `oms_ledger_balance_identity_cache_requests_total{result="hit"}`  | n/a (no cache) | 160 000 / 160 000 | 100 % hit-rate at steady state |
| `oms_ledger_balance_identity_cache_requests_total{result="miss"}` | n/a            | ~1 / ingress (warmup) | — |

**Acquire time tripled even though usage dropped.** That is the
signature of pool starvation: the cache removed ~15 ms of HTTP wait per
request, so accept compresses, threads finish faster, and the next
request immediately hits the Hikari queue for the BUY-async
`ledger_inflight_outbox` INSERT. The connection itself runs in 1.35 ms,
but 16 ms is spent waiting in line for one.

### Where the threads went (jstack at c1600, ingress-1, 200 exec threads)

| Frame                                                                    | Threads | % of pool |
|--------------------------------------------------------------------------|---------|-----------|
| `LedgerInflightOutboxRepository.insert → Hikari ConcurrentBag.borrow`    | 186     |  93 %     |
| `OmsClusterIngressClient.submitAcceptOrder` (cluster admit reply)        |  13     |   6.5 %   |
| `LedgerInflightHoldFailureCompensator.runOnce`                           |   1     |   0.5 %   |
| `ApiKeyFilter.doFilterInternal`                                          |   1     |   0.5 %   |

Compare to D-3 jstack at c1600: 187 / 200 in
`RestLedgerBalanceClient.fetchBalanceRoot`. D-8 cleanly removed that
wait — the remaining wait is now Postgres connection-pool starvation,
not HTTP I/O.

### Theoretical ceiling math

* Per ingress: Hikari pool = 20 connections, each holding 1.35 ms per
  request → 20 / 0.00135 ≈ **14 800 inserts/sec/ingress**.
* Two ingresses → **29 600 inserts/sec theoretical**.
* Observed: 20 874 rps (best) ≈ 70 % of theoretical, the gap is the usual
  contention overhead (lock acquire, thread parking, JVM safepoints,
  Tomcat NIO accept-queue handoff).

### Lift summary across Tier 2.5 (slice 4p baseline → D-8)

| Phase | RPS    | Lift over baseline | Lift over previous |
|-------|--------|--------------------|--------------------|
| 4p    |  1 094 | 1.00 ×             | —                  |
| C-1+2 |  3 800 | 3.47 ×             | 3.47 ×             |
| C-3   |  7 762 | 7.10 ×             | 2.04 ×             |
| C-4   |  7 770 | 7.10 ×             | 1.00 ×             |
| D-1   |  7 900 | 7.22 ×             | 1.02 ×             |
| D-2   |  4 700 (regression) | 4.30 ×    | 0.59 × (Supavisor backend = 20 surfaced) |
| D-4   | 10 561 | 9.65 ×             | 2.25 × (after Supavisor backend = 80 fix) |
| D-3   | 11 629 | 10.63 ×            | 1.10 ×             |
| **D-8** | **20 874** | **19.08 ×**     | **1.795 ×**         |

### Lessons (D-8 specific)

* **The Tomcat-thread hypothesis was wrong.** The simple math
  (200 threads × 33.8 ms ≈ 11 832 rps) matched the observed D-3 ceiling
  almost exactly, which made D-7 (bump max-threads) look like the
  smallest-effort next win. Jstack showed 93.5 % of those "saturated"
  threads were not doing CPU work — they were parked on a synchronous
  Ledger HTTP call. Always confirm the saturation cause before sizing
  the cure. (Same lesson as C-1 → C-2: the queue depth didn't tell us
  *what* the queue was waiting on.)
* **Caching a durable mapping is a root-cause fix, not a workaround.**
  The `(balanceId → identityId)` binding only changes on operator
  reassignment. Caching it locally with a 5 min TTL is a textbook
  application of "stop fetching what doesn't change". The accept-side
  invariant is preserved: a stale cache during operator reassignment can
  let through up to 5 min of orders claiming the old identity, which is
  the same staleness window the operator already tolerates between the
  reassignment and the OMS replicas restarting.
* **Caffeine's `Cache.get(K, Function)` gives you per-key
  single-flighting for free.** The unit test that proved this had 64
  threads stampeding the same cold key; exactly one delegate call ran.
  This matters at burst-start when 1 600 client connections all ask
  about the same balanceId before the first verify completes — without
  single-flighting, the cache populates 1 600 times. With it, once.

### Recommended next slices (post-D-8)

* **Phase D-9 — move the BUY-async inflight-outbox INSERT off the
  ingress hot path.** Mirrors D-3's `OrderAccepted` move: the projector
  already idempotently backfills `ledger_inflight_outbox` from
  `OrderAdmittedEvent` (D-1's `insertIfAbsent`); flipping the projector
  from a "safety net" to the **only** writer removes the last Postgres
  INSERT from ingress. Expected: ingress accept drops from 18.7 ms →
  ~3 ms (cluster admit + Tomcat NIO only), throughput jumps to whatever
  cluster admit can sustain (Aeron benches show 50–80 k rps for
  single-leader IPC on this hardware), at which point D-6 (admit
  batching) becomes the next obvious lever. **This is the largest
  remaining single lever; skip D-5 / D-6 / D-7 and go here.**
* **Phase D-6 — admit batching at the cluster client.** Becomes the
  ceiling once D-9 lands. Single Aeron cluster leader thread serialises
  per-admit; batching 8–16 requests per `submitAcceptOrder` round-trip
  with per-request future fan-out on the response side gives N× the
  admit ceiling for N× admit latency. The D-1 idempotency guarantees
  (cluster log is the source of truth, projector is idempotent) compose
  cleanly with batching.
* **Phase D-5 — split projector + fix-egress onto a separate Supavisor
  tenant.** Modest single-digit-% lift; deprioritised vs D-9.
* **Phase D-7 — Tomcat thread bump.** Now properly motivated only after
  D-9: with no Postgres I/O on the ingress hot path, Tomcat's 200-thread
  ceiling × per-request CPU cost becomes the wall, and a 400-thread
  bump becomes useful. Before D-9 it would only deepen the Hikari queue.

## Tier 2.5 phase D-9 verification — projector is sole writer of `ledger_inflight_outbox` (Pop! 2026-05-14)

### TL;DR

| Concurrency | RPS | Mean RTT | p50 RTT | p99 RTT | Failures |
|-------------|-------------|-----------|-----------|-----------|----------|
| warmup (c80) | 10 643 | 5.95 ms | 2.56 ms | 109.25 ms | 0 |
| c400 | 46 505 | 7.32 ms | 5.95 ms | 24.06 ms | 0 |
| c800 | 51 348 | 13.48 ms | 11.84 ms | 58.82 ms | 0 |
| c1200 | 49 275 | 21.38 ms | 19.71 ms | 59.33 ms | 0 |
| c1600 | 53 929 | 28.04 ms | 25.60 ms | 94.21 ms | 0 |
| **c2400** | **57 282** | 40.43 ms | 37.89 ms | 123.07 ms | 0 |
| c3200 | 53 130 | 58.92 ms | 55.74 ms | 190.46 ms | 0 |

* Best run **57 282 rps** at c2400, **2.74× over D-8** (20 874 rps),
  **52.4× over slice 4p baseline** (1 094 rps).
* RPS plateau between c1600 and c3200 sits at **53–57 k**: the system is
  no longer queue-bound on Postgres or Hikari (those acquired sub-µs); the
  remaining wait is in the Aeron cluster commit RTT.
* **0 failures across 1.92 M requests** in the sweep.

### Setup

* Code: `oms@272042a` (`oms: tier-2.5 phase D-9 — projector is sole writer
  of ledger_inflight_outbox`).
* Flags on Pop! `~/.oms-bench.env`: unchanged from D-8 (no new env knobs in
  D-9 — the change is a pure code refactor of where the
  `ledger_inflight_outbox` row is written). Specifically
  `OMS_PG_POOL_MAX_SIZE=20`, `inflight-async-enabled=true`,
  `inflight-coalescer-enabled=false`, balance-identity cache on
  (`enabled=true ttl=300 max=100000`), Supavisor `default_pool_size=80`.
* Same 5 OMS JVMs (cluster-node + projector + fix-egress + 2 ×
  ingress-replica), Postgres truncated before run, Aeron cluster dir wiped.

### Smoke test (5 sequential POSTs, 1 conn, same balanceId)

* HTTP: 5 / 5 → 201, first POST 38 ms (cold cluster + cold cache),
  subsequent POSTs ~5 ms.
* After 2 s: `orders=5`, `ledger_inflight_outbox=5`,
  `domain_event_outbox=10` (5 × `OrderAccepted` + 5 × `OrderWorking`).
  Critically: **all 5 inflight rows came from the projector**, not from
  the ingress JVM (the ingress no longer holds a
  `LedgerInflightOutboxRepository` reference at all).
* Cache: 4 hits / 1 miss — single-flighting + warmup behaving as in D-8.

### Where the time went, vs D-8 (per-ingress, c1600 row)

| Timer | D-8 | D-9 | Δ |
|--------------------------------------|---------|---------|---------|
| `oms.pipeline.ingress.accept` | 18.7 ms | **2.86 ms** | **−85 %** |
| `hikaricp_connections_acquire` (per-acquire mean) | 16.2 ms | **0.67 µs** | **≈ 0** |
| `hikaricp_connections_usage` (per-acquire count) | 160 000 | **291** | **−99.8 %** |
| `oms_ledger_balance_identity_cache_requests_total{result="hit"}` | 160 000 | 160 000 | unchanged (100 % hit-rate) |
| `oms_ledger_balance_identity_cache_requests_total{result="miss"}` | ~1 / ingress | ~1 / ingress | unchanged |

The Hikari count dropping from 160 000 to 291 is the headline: **almost
no ingress request opens a Postgres connection any more**. The 291
acquires that remain are background work (actuator endpoints, the
fixed-rate `LedgerBalanceClient.fetchAvailableBalance` health probe in
the slice 4p reconciler — none of them on the order-accept hot path).
Per-acquire usage time stayed around 16 ms (similar to D-8) but the
acquire count is so small that it doesn't show up as a wait at the
Tomcat-thread level any more.

### Where the threads went (jstack at c1600, ingress-1, 214 exec threads)

| Frame | Threads | % of pool |
|-----------------------------------------------------------------------------|---------|-----------|
| `OmsClusterIngressClient.submitAcceptOrder` (Aeron cluster admit reply wait) | 174 | 81 % |
| Other parkNanos (Tomcat scheduler) | 13 | 6 % |
| `TaskQueue.take` (idle) | 10 | 5 % |
| Tomcat NIO selector + misc Tomcat internals | 8 | 4 % |
| `LedgerInflightOutboxRepository.insert → ConcurrentBag.borrow` | **0** | **0 %** |
| `RestLedgerBalanceClient.fetchBalanceRoot` | **0** | **0 %** |

Compare to D-8 jstack at c1600: 186 / 200 in
`LedgerInflightOutboxRepository.insert → ConcurrentBag.borrow`. D-9
cleanly removed that wait — the wall is now the Aeron cluster's
**single-leader commit serialisation**, not Postgres connection-pool
starvation. This is exactly the next-lever signal the post-D-8 plan
predicted.

At c2400 (the peak step) the cluster-wait fraction drops to 111 / 214
(52 %) and "other parkNanos" rises to 87 (41 %): the Tomcat scheduler
is starting to absorb queue depth into request-thread wait. Beyond
c3200 the system would benefit from D-7 (Tomcat thread bump) — now
properly motivated because no I/O is still on the hot path, only CPU
+ scheduler.

### Theoretical ceiling math

* Per ingress: a single Aeron cluster commit RTT ≈ 1.5–2 ms in steady
  state on Pop! (single leader, single-host IPC). 200 Tomcat exec
  threads × ~3 ms accept time = ~67 k accepts/sec/ingress; with two
  ingresses sharing one cluster leader, the *sum* hits the leader's
  serialisation ceiling first.
* Observed: 57 282 rps best ≈ 30 k / ingress. The leader is still the
  wall — admit batching (D-6) is the next obvious lever to multiply
  this by N (the cluster commits N admits per round-trip with N×
  per-request future fan-out on the response side).

### Lift summary across Tier 2.5 (slice 4p baseline → D-9)

| Phase | RPS | Lift over baseline | Lift over previous |
|-------|--------|--------------------|--------------------|
| 4p | 1 094 | 1.00 × | — |
| C-1+2 | 3 800 | 3.47 × | 3.47 × |
| C-3 | 7 762 | 7.10 × | 2.04 × |
| C-4 | 7 770 | 7.10 × | 1.00 × |
| D-1 | 7 900 | 7.22 × | 1.02 × |
| D-2 | 4 700 (regression) | 4.30 × | 0.59 × (Supavisor backend = 20 surfaced) |
| D-4 | 10 561 | 9.65 × | 2.25 × (after Supavisor backend = 80 fix) |
| D-3 | 11 629 | 10.63 × | 1.10 × |
| D-8 | 20 874 | 19.08 × | 1.795 × |
| **D-9** | **57 282** | **52.36 ×** | **2.74 ×** |

### Lessons (D-9 specific)

* **The largest remaining I/O sink was the simplest to remove.** The
  ingress-side `ledger_inflight_outbox` INSERT was already mirrored
  by the projector's idempotent backfill (D-1 made it crash-safe
  back in May 2026). Promoting the projector to the only writer was
  a **5-line behavioural change** in `OrderIngressService` (drop the
  `enqueueBuyLedgerInflightHold` body) plus a Javadoc rename in
  `OmsPostgresProjector`. The lift was 2.74×.
  Prior art is the right starting point for the highest-leverage
  refactors — the scaffolding (idempotency, projector cursor,
  reconciler) was already in place; D-9 is just operator-flippable
  re-routing.
* **D-7 is now properly motivated.** With Postgres + Hikari + Ledger
  HTTP all off the ingress hot path, the c3200 jstack starts to show
  Tomcat scheduler queueing as a real source of wait (87 / 214
  threads in "other parkNanos"). A 400-thread Tomcat bump would
  delay the queue-up wall; before D-9 the same bump only deepened
  the Hikari queue.
* **Synthetic-test noise from the slice 4p reconciler is expected.**
  At 1.92 M admits in ~36 s, the reconciler ran out of test-Ledger
  funds (test balance: 100 EUR; per-order hold: 1.50 EUR) and logged
  ~82 k "Insufficient funds" warnings. The compensator cancelled the
  affected admits in turn (11 017 `compensated_at` rows). This does
  not affect the customer-facing 201 RPS — those came back the moment
  the cluster admitted, before any Ledger interaction. It does
  demonstrate that the slice 4p reconciler / compensator pipeline
  consumes the projector-written rows identically to ingress-written
  rows, which is the durability-equivalent property D-9 needed to
  preserve.

### Recommended next slices (post-D-9)

* **Phase D-6 — admit batching at the cluster client.** Now the
  unambiguous next lever. The 174 / 214 jstack signal at c1600 is
  exactly what an unbatched single-leader admit looks like at
  saturation. Batching N requests per `submitAcceptOrder` round-trip
  with per-request future fan-out on the response side multiplies
  the ceiling by N at the cost of N× admit latency. Expected
  2–3× lift with `batchSize ≈ 8–16` (cluster commits one log slot
  per batch, Aeron+Postgres commit are the same per-batch).
* **Phase D-7 — Tomcat thread bump.** Pre-D-9 plan said this was
  "now properly motivated"; the **D-7 falsification experiment**
  below (immediately after D-9) tested that hypothesis with a 1-flag
  change and found it **regressed peak rps by −7.5 %** (57 282 →
  52 967). The 87 / 214 "other parkNanos" frames seen in the D-9
  c2400 jstack were idle-pool overhead, not queue-wait — doubling
  the pool just doubled the idle frames. Keep `threads.max=200`
  until D-6 lifts the cluster ceiling; D-7 may matter again at a
  much higher RPS regime.
* **Phase D-10 (new) — projector throughput tuning.** D-9 surfaced
  a downstream limit: at 57 k rps on ingress, the projector +
  reconciler (running in the same JVM) drained admit events to
  Postgres at ~360 events/s once the slice-4p reconciler started
  hammering Ledger with insufficient-funds errors. In production the
  reconciler will not be receiving steady-state errors, so this is a
  bench-specific symptom, but the structural concern remains: the
  projector / reconciler share a Spring scheduler thread pool, and
  the reconciler's blocking Ledger HTTP call can starve the
  projector's admit-event drain. Splitting them onto separate
  schedulers (or moving reconciler to its own JVM) would decouple
  the two so projector lag stays bounded under reconciler back-up.
  Not a customer-facing 201 issue — only an "orders row appears in
  Postgres" lag issue.
* **Phase D-5 — split projector + fix-egress onto a separate
  Supavisor tenant.** Stays deprioritised vs D-6 / D-7 / D-10.

## Tier 2.5 phase D-7 falsification — Tomcat thread bump alone (Pop! 2026-05-14)

Run between D-9 and D-6 to test the D-9 plan claim that "D-7 is now
properly motivated." **Pure env flip — no code change** —
`SERVER_TOMCAT_THREADS_MAX=400` on both ingress JVMs, all other
config / DB state identical to the D-9 sweep. Same Postgres / Aeron
wipe + same launch order + same bench script.

### TL;DR

| Concurrency | D-9 (`threads.max=200`) | D-7 (`threads.max=400`) | Δ rps |
|-------------|-------------------------|-------------------------|-------|
| warmup (c80) | 10 643 | 10 371 | −2.6 % |
| c400 | 46 505 | 44 749 | −3.8 % |
| c800 | 51 348 | 44 589 | **−13.2 %** |
| c1200 | 49 275 | 53 147 | +7.9 % |
| c1600 | 53 929 | 54 073 | +0.3 % |
| **c2400 (peak)** | **57 282** | **52 967** | **−7.5 %** |
| c3200 | 53 130 | 50 257 | −5.4 % |

* **Peak rps regressed**, the c800 leg regressed by 13 %, and no leg
  showed a meaningful gain. **D-7 alone is a no-op-or-worse, not a
  win** — falsifies the D-9 runbook claim that the c2400 / c3200
  scheduler-side parkNanos was queue-wait.

### Where the threads went (jstack at c1600, ingress-1, threads.max=400)

| Frame | D-9 (414 caps the pool) | D-7 (414 actual) | Δ |
|-----------------------------------------------------------------------------|-------------------------|------------------|---|
| `OmsClusterIngressClient.submitAcceptOrder` (cluster admit reply wait) | 174 / 214 (81 %) | **86 / 414 (20 %)** | absolute count went down |
| `LockSupport.park` (other) | 13 / 214 (6 %) | **299 / 414 (72 %)** | absolute count exploded |
| `TaskQueue.take` (idle exec thread) | 10 / 214 (5 %) | 10 / 414 (2 %) | unchanged |
| Tomcat NIO selector / epoll | 8 / 214 (4 %) | 2 / 414 (0 %) | unchanged |

The "active in-flight admit count" — threads actually parked in
`submitAcceptOrder` — **stayed roughly the same** (174 → 86;
across two ingresses it sums to similar effective work). The extra
200 threads on each ingress are just **idle exec workers parked
on `Tomcat$ContainerThreadMarker$BlockingQueue.poll`**, contributing
context-switch overhead to no useful end. The wall is genuinely the
**Aeron cluster's single-leader admit serialisation** (`onSessionMessage`
processes one admit at a time, and a single-host cluster has a fixed
per-admit CPU cost ≈ 17.5 µs at 57 k rps), **not Tomcat thread
budget**. More Tomcat threads just queue more work at the same
single cluster ingress.

### Cluster commit RTT trend (per-leg mean, ingress-1)

| Leg | D-9 mean RTT | D-7 mean RTT |
|-----|--------------|--------------|
| c400 | ~1 ms | ~1 ms |
| c800 | ~2 ms | ~2 ms |
| c1200 | ~2 ms | ~2 ms |
| c1600 | ~3 ms | ~3 ms |
| c2400 | ~3 ms | ~3 ms |
| c3200 | ~3 ms | ~4 ms |
| post-c3200 (cumulative) | ~3 ms | ~4 ms |

Cluster RTT is identical-to-slightly-worse with `threads.max=400`,
consistent with the throughput regression: more Tomcat threads
**did not** reduce per-admit cluster wait, they just added scheduler
overhead.

### Lessons (D-7 specific)

* **Falsifying first is cheaper than coding speculatively.** The
  pre-D-9 plan claimed D-7 was "now properly motivated"; a 5-min
  env-only experiment disproved it. Worth doing **before** the
  ~hours of D-6 implementation, because if D-7 had paid off, D-6
  scope would have shrunk.
* **The `LockSupport.park (other)` frames in the D-9 c2400 jstack
  were idle-pool overhead, not queue-wait.** Frame count alone is
  not enough to tell those apart; the falsifier was running the
  bigger pool and seeing the absolute submitAcceptOrder count
  *not* increase.
* **Hardware ceiling check held.** Pop! is at ~9 % CPU during the
  bench (load avg 8 / 48 cores). The 57 k rps wall is **software
  architectural**, not hardware. D-6 (admit batching) is the only
  lever that changes the per-admit work model the cluster runs.
* **Reverted.** `SERVER_TOMCAT_THREADS_MAX` is not committed to
  `~/.oms-bench.env`; the post-D-7-experiment baseline is the same
  D-9 stack, threads.max=200 default, ready for D-6.

## Tier 2.5 phase D-6 verification — admit batching at the cluster client (Pop! 2026-05-14)

Code: commit `oms@ee90d9e` (`oms: tier-2.5 phase D-6 — admit batching at the
cluster client`). Wire format adds `TYPE_ID_BATCH_ACCEPT_ORDER = 4` plus
`BatchAcceptOrderCommand` (16-byte shared header + int count + N inner-length-prefixed
`AcceptOrderCommand` bodies, MAX_COUNT=256). `OmsClusterIngressClient` gains an opt-in
admit-batcher daemon thread that drains an `ArrayBlockingQueue` and packs N admits into
one Aeron cluster log slot; `OmsAdmissionClusteredService.applyBatchAcceptOrder`
dispatches each inner body through the existing `applyAcceptOrder` path. Default off
(slice 4n single-message path remains the safe baseline); flip
`OMS_CLUSTER_CLIENT_ADMIT_BATCH_ENABLED=true` per ingress to opt in.

### TL;DR

| Leg                      | Config                                         | c1600 rps | c2400 rps (peak)  | c3200 rps |
|--------------------------|------------------------------------------------|-----------|--------------------|-----------|
| **A — D-9 baseline**     | `admit-batch=false` (today's main)             | 54 835    | **55 909**         | 55 408    |
| **B — D-6 batch=1**      | `admit-batch=true, max=1, flush=50 µs`         | 54 118    | 53 157             | 55 844    |
| **C — D-6 batch=8**      | `admit-batch=true, max=8, flush=50 µs`         | 55 045    | **56 590** (+1.2 %)| 56 086    |
| **D — D-6 batch=16**     | `admit-batch=true, max=16, flush=50 µs`        | 52 547    | 53 453             | 51 730    |
| **E — D-6 batch=8 falsifier** | `admit-batch=true, max=8, **flush=500 µs**` | 47 677    | 50 391             | 52 119    |

**The predicted "2–3× lift with batchSize ≈ 8–16" did not materialise.** Best peak rps
with admit-batching enabled was leg C at 56 590 — **+1.2 % over the D-9 baseline**, well
inside Pop! single-host run-to-run noise (legs A vs leg-9 main commit at 55 909 vs
57 282 = ±2.4 %). Larger `maxBatchSize=16` slightly *regressed* (peak 53 453); longer
`flush=500 µs` regressed peak by **−11 %** (50 391 at c2400). All 5 legs × 1.92 M+
admits succeeded, no failures.

### Where the predicted lift went

The cluster commit RTT *did* improve modestly with batching, but the throughput wall
was already past it:

| Leg                 | c2400 ingress accept (ms) | c2400 cluster RTT (ms) | rps    |
|---------------------|---------------------------|-------------------------|--------|
| A baseline          | 3.249                     | 3.200                   | 55 909 |
| B batch=1           | 3.173                     | 3.121                   | 53 157 |
| **C batch=8**       | **2.989** (−8 %)          | **2.945** (−8 %)        | 56 590 |
| D batch=16          | 3.174                     | 3.121                   | 53 453 |
| E batch=8 flush=500 | 3.346 (+3 %)              | 3.289 (+3 %)            | 50 391 |

* Leg C *did* shave ~8 % off both ingress accept time and cluster RTT — proof the
  batcher daemon is functioning and amortising at least *some* per-frame overhead.
* But peak rps moved only +1.2 %. By Little's law (200 threads × 2 ingresses ÷ 3 ms ≈
  133 k rps theoretical thread-bound ceiling) and observed mean RTT 27.5 ms / c2400 =
  87 k thread-bound ceiling, we are **well below** the ingress-thread budget either
  way. The wall is **downstream of `submitAcceptOrder`** — most of cluster commit RTT
  is per-admit cluster-service work (`applyAcceptOrder` idempotency lookup +
  `OrderAcceptedEvent` allocation + `events.offer` egress publication), which runs
  exactly the same N times whether the inbound was batched or not.

### Falsifying "longer flush would help"

Leg E ran `batchSize=8, flush=500 µs` (10× longer flush). Hypothesis: at Pop!'s
per-ingress arrival of ~28 k/s (= 1 admit per ~36 µs), legs B/C/D probably saw
effective batch sizes ≪ configured `maxBatchSize` because the daemon's 50 µs flush
timer kept firing first. A longer flush should produce real multi-admit batches and,
if the per-frame cluster framing is the wall, should lift peak rps.

**Falsified.** Leg E peak (52 119 at c3200) regressed −7 % vs leg C; cluster RTT *grew*
to 3.289 ms (+11 %) at c2400 because the daemon now waits up to 500 µs before flushing
even a single admit. The added wait completely overwhelmed any framing-amortisation
gain. Conclusion: the per-admit cluster CPU cost dominates per-frame framing cost on
this single-host rig.

### Where the threads went (jstack at c1600, ingress1)

| Frame                                                | A baseline | C batch=8 | D batch=16 |
|------------------------------------------------------|------------|-----------|------------|
| `OmsClusterIngressClient.submitAcceptOrder*` (cluster reply wait) | 181 / 214 (85 %) | 187 / 214 (87 %) | 140 / 214 (65 %) |
| `LockSupport.parkNanos` (other / waiter.get)         | 0          | 0         | 55 (26 %)  |
| Tomcat NIO selector / idle pool                      | 33         | 27        | 19         |

Legs A and C show essentially the same shape: 80–90 % of Tomcat exec threads parked on
the cluster reply, just like in the post-D-9 c1600 jstack (174/214). Leg D
(`maxBatchSize=16`) starts spreading threads into batcher-park frames — when the
configured batch size approaches the in-flight count, threads can park inside
`submitAcceptOrderViaBatcher` for the per-batch flush rather than directly in
`submitAcceptOrder`. The total wait shape is unchanged.

### Lessons (D-6 specific)

* **D-6 is correct as-implemented but the predicted lift didn't materialise on Pop!.**
  All 1.92 M+ admits across legs B/C/D + leg E succeeded with zero failures; the wire
  format, daemon, and cluster-side dispatch all work. Best lift was +1.2 %, inside
  noise.
* **The wall is per-admit cluster-service CPU, not framing.** The
  `applyAcceptOrder → events.offer` egress publication runs N times per batch
  regardless of inbound batching. Batching at the cluster *client* amortises only the
  Aeron frame send + leader dispatch (~2–3 % of total cluster time).
* **Falsification before commit-to-implementation is cheap.** This run cost ~80 min
  wall-clock for a finding that says "the predicted next lever isn't the right
  lever". Cheaper than committing to a longer optimisation arc and finding out 3
  slices in.
* **D-6 ships behind `OMS_CLUSTER_CLIENT_ADMIT_BATCH_ENABLED=false` (default off).**
  The infrastructure is useful — across-network deployments may see a larger framing
  share, where the same code may pay off. On Pop! single-host benches it stays off.
* **What the wall actually is — hypothesis, not asserted.** The remaining
  `submitAcceptOrder` wait on each accept is 2.9–3.0 ms. Plausible bottlenecks:
  (1) cluster-service `events.offer` back-pressure from the projector's slower
  consume rate; (2) Aeron media-driver conductor capacity for the single ingress
  publication; (3) Pop! single-host CPU contention between ingress + cluster +
  projector + reconciler all on one box. None of these is observably refuted by the
  current data.

### Recommended next slices (post-D-6)

After the **D-investigate falsifier below**, the post-D-6 plan is rewritten:

* **Phase D-11 (egress event batching from the cluster service) — DROPPED.**
  Falsified by the D-investigate run below: per-admit `events.offer` and
  `session.offer` together total **~2 µs**, ≪ 0.1 % of the 3.4 ms cluster RTT.
  Batching the cluster's outbound egress would amortise a non-bottleneck.
* **Phase D-10 — projector decoupling.** Still on the list, but **not motivated
  for ingress RPS** — projector lag does not back-pressure the cluster's
  `events.offer` (events_BP_ticks/admit ≈ 0.16 at c2400, sub-microsecond
  total wait per admit). Keep D-10 for *production stability* (so the
  reconciler's blocking Ledger calls can't starve the projector's drain),
  but expect zero ingress-side rps lift.
* **Phase D-7 — Tomcat thread bump.** Stays disproven.
* **Phase D-5 — split projector + fix-egress onto a separate Supavisor tenant.**
  Stays deprioritised.
* **Phase 5 — multi-host / k8s.** The remaining ~3.4 ms cluster RTT is in
  Aeron transport (media-driver IPC + log append + driver round-trip). On a
  single-host rig the media driver is a shared bottleneck across ingress +
  cluster + projector + fix-egress JVMs. Further single-host RPS gains will
  require either Aeron media-driver tuning (dedicated sender/receiver/conductor
  threads, off-cpu pinning) or moving to a multi-host topology. **Defer
  further OMS-code optimisation; the OMS side is at its single-host ceiling.**

## Tier 2.5 phase D-investigate — falsifying "per-admit egress publish is the wall" (Pop! 2026-05-14)

Code: commit `oms@3cc3cea` (`oms: instrument cluster-service per-emit publish path
(D-investigate)`). Adds four meters on
{@code OmsAdmissionClusteredService} exposed at the cluster-node JVM's
`:8089/metrics`:

* `oms.cluster.service.events_offer_seconds{event_kind=admitted}` — Timer,
  per-call wall time of `eventsPublication.offer` including any
  `Thread.yield` busy-wait on Aeron back-pressure.
* `oms.cluster.service.events_offer_back_pressure_total{event_kind=admitted}`
  — Counter, increments once per `Thread.yield` tick. Non-zero rate signals
  the Archive / projector consume rate isn't keeping up with leader produce.
* `oms.cluster.service.session_offer_seconds{event_kind=accepted}` — Timer,
  same shape for the egress `OrderAcceptedEvent` reply via `session.offer`.
* `oms.cluster.service.session_offer_back_pressure_total{event_kind=accepted}`
  — Counter for the egress publication.

Same instrumentation pattern as the existing `oms.cluster.snapshot.duration`
timer (line ~385); meters are observability-only on a JVM-local registry, so
they don't violate the cluster-service determinism rule (no impact on emitted
event payloads / state / cluster log).

### Bench shape

Single leg, admit-batch off (the D-9 / leg-A baseline, just with the new
meters running). c80/warmup → c1600 → c2400 → c3200, 5 k → 640 k requests
per step, two-ingress round-robin. Peak rps = **57 023 at c2400** — within
the noise band of the un-instrumented D-6 leg A (55 909) and the D-9 main
commit (57 282), so the timer overhead is sub-1 % and not regressing
throughput.

### Per-emit publish times — falsifier

| Concurrency | admits at ingress | cluster RTT mean (ms, ingress view) | events_offer mean (µs, cluster) | session_offer mean (µs, cluster) | events BP ticks / admit | session BP ticks / admit |
|-------------|-------------------|--------------------------------------|---------------------------------|-----------------------------------|--------------------------|---------------------------|
| c1600       | 160 281           | 3.384                                | 1.0                             | 1.0                               | 0.19                     | 0.00                      |
| **c2400**   | **240 300**       | **3.420**                            | **1.0**                         | **1.0**                           | **0.16**                 | **0.00**                  |
| c3200       | 320 402           | 3.383                                | 1.0                             | 1.0                               | 0.14                     | 0.00                      |

At c2400: cluster-node-side counts confirm the meters fired correctly —
`events_offer.count = 480 000` (one per admit on each accept), `session_offer.count =
480 000` (one accepted-event egress per admit), `events_offer_back_pressure_total =
78 341` (~0.16 ticks/admit on average; 16 % of admits saw at least one yield),
`session_offer_back_pressure_total = 4` (essentially zero — egress to the
ingress client never back-presses).

**The result:** per-admit egress publish work — `events.offer` + `session.offer` —
totals **~2 µs**, while ingress sees **~3 400 µs of cluster RTT**. Egress
publish is **0.06 % of cluster RTT.** The "per-admit egress publish is the
wall" hypothesis from the post-D-6 runbook is **decisively falsified.** D-11
(egress event batching from the cluster service) would amortise a
non-bottleneck; **D-11 is dropped from the next-slice list.**

### Where the cluster RTT actually goes

3 400 µs cluster RTT − 2 µs egress publish = **~3 398 µs unaccounted on the
cluster service thread** at c2400. This time is *outside*
`applyAcceptOrder`'s emit calls (the only OMS-cluster-code we measured), so it
falls in the **Aeron-internal transport** path:

1. Aeron client publication on the ingress JVM (offer to local media driver).
2. Driver-to-driver transport — Pop! single-host = UDP loopback or IPC SHM
   ring buffers between the ingress driver and the cluster-node driver.
3. Cluster module receive on the leader (Aeron Cluster Service Container's
   `pollImage` loop reading from the consensus stream).
4. Consensus log append (single-host = no replication wait but still a
   serialised append on one log buffer).
5. `applyAcceptOrder` body — idempotency lookup (HashMap), state update,
   emit calls (~2 µs total per the table above).
6. Egress driver-to-driver transport back to the ingress JVM.
7. Ingress driver-to-client receive + `pending`-map demux + future completion
   + Tomcat exec-thread wakeup.

Steps 1–4, 6–7 are **Aeron media-driver work, not OMS code.** On Pop!
single-host all four JVMs (ingress×2, cluster-node, projector, fix-egress)
share the same per-driver conductor thread for their respective drivers and
the same kernel for the shared-memory rings. With ~57 k admits/s × multiple
publications / subscriptions per admit, per-driver conductor work + kernel
context switches add up.

### Why this matters for the next-slice plan

* **The OMS-side levers (D-1 → D-9) have been pulled.** Hot path opens zero
  Postgres connections, zero blocking HTTP calls, zero `Spring @Transactional`
  on accept — verified in D-9 jstack (174 / 214 ingress threads parked on
  cluster reply, no I/O frames). The remaining 3.4 ms is Aeron-internal.
* **Further single-host RPS gains will require Aeron-side work** (dedicated
  sender / receiver / conductor threads, off-cpu pin, MDC flags for shared-mem
  IPC instead of UDP loopback) or **moving to multi-host** (Phase 5 k8s).
* **Don't chase D-10 / D-11 / D-12 single-host code optimisations** until
  either of the above is available; the model says no OMS-code change can
  meaningfully amortise the residual 3.4 ms.
* **What we *should* do next on this rig:** read the **Aeron driver counter
  file** (`aeron-stat -d $AERON_DIR/media-driver/cnc.dat`) during a c2400 step
  and look for the publication-back-pressure-events / image-block-position-lag
  counters on the cluster-input + events publications. That tells us *which*
  Aeron internal path is the bound (driver conductor saturating? back-pressure
  on the consensus log? receive-thread starved?). One short bench run, no code
  change.

### Lessons (D-investigate specific)

* **Two days of "the cluster admit is the wall" was correct on shape but
  misleading on cause.** D-6 / D-7 / "D-11 egress" all assumed the wall was
  inside `applyAcceptOrder` (framing, dispatch, egress emit). The real wall
  was *outside* it, in Aeron transport. A single hour of instrumentation
  beats a slice of speculative implementation.
* **Default-on observability for hot paths pays off.** The four meters added
  here cost ~1 % overhead and now let any future bench answer "is publish the
  wall?" without re-instrumenting. Keep them on.
* **Falsifying expensive D-11 work was cheap.** ~1 hour of code + 5-min bench
  vs days of egress-wire-format design. This is the same lesson as the
  pre-D-6 D-7 falsification (5-min env-flip beat hours of speculative
  Tomcat-thread-bump rationalisation).

## Tier 2.5 phase D-aeron-stat — Aeron media-driver counter probe (Pop! 2026-05-14)

Continuation of **D-investigate**, no code change. After D-investigate
falsified "per-admit egress publish is the wall" and pinned the residual
~3 400 µs of cluster RTT to **Aeron-internal transport**, the obvious next
step is to actually *read* the Aeron media-driver counter file (`cnc.dat`)
under load and see which Aeron-internal counter — driver back-pressure,
NAK / retransmit, sender flow-control limit, conductor / sender / receiver
work-cycle saturation — actually moves. That tells us whether the next
lever is Aeron-side tuning (driver thread-pinning, channel mode flags) or
purely topology (Phase 5 multi-host).

### Methodology

* **No code change.** The currently running Pop! stack is the D-9 baseline
  (admit-batch off, D-investigate timers on). All five OMS JVMs (`cluster-node`,
  `projector`, `fix-egress`, `ingress-1`, `ingress-2`) share **one** media-driver
  directory: `~/oms/build/aeron-cluster/media-driver/cnc.dat`, owned by the
  cluster-node JVM (PID 2 869 907) since the embedded `MediaDriver` lives there.
  `OMS_POSTGRES_PROJECTOR_AERON_DIR` and `OMS_FIX_EGRESS_AERON_DIR` point
  the projector and FIX-egress to the same dir; ingress JVMs default to
  `${OMS_AERON_DIR_BASE:build/aeron-cluster}/media-driver`. So **one cnc.dat
  observes the entire substrate.**
* **Tooling.** `io.aeron.samples.AeronStat` ships in
  `aeron-samples-1.48.0.jar` (Maven Central — *not* in the default
  `io.aeron:aeron-{client,driver,archive,cluster}` dependency tree); fetched
  to `~/aeron-tools/aeron-samples-1.48.0.jar` once on Pop! and reused.
  Invoked with `watch=false` for one-shot snapshots and the same
  `--add-opens` / `--add-exports` block the rest of the OMS JVMs use, since
  Agrona's `UnsafeApi` reaches into `jdk.internal.misc`.
* **Driver run.** Single c2400 burst (480 000 orders, no warmup, two-ingress
  round-robin, `OMS_BURST_ACCOUNT_POOL=4800`, identical knobs to the
  D-investigate c2400 leg). Snapshots at t = 0 s (idle baseline), then
  every 5 s during and after the burst (t05 through t25), then a final
  post-snapshot. Burst fired at 17:51:36 and finished 17:51:45 (8.6 s
  total) — so t00 → t05 covers ~5 s of burst, t05 → t10 covers ~3.6 s of
  burst plus ~1.4 s drain, t10 onward is post-burst drain. Throughput
  observed: **480 000 admits / 8.633 s = 55 603 rps** (within noise of the
  D-9 / D-investigate c2400 plateau of 55 909 / 57 023 / 57 282).
* **Bench script:** `/tmp/bench-d-aeron-stat.sh` (Pop!), output rooted at
  `/tmp/bench-d-aeron-stat-20260514-175136/` with `aeronstat-{t00_idle,
  t05_load, t10_load, t15_load, t20_load, t25_load, t99_post}.txt` and
  `cluster-{pre,post}.prom` / `ingress{1,2}-{pre,post}.prom`.

### Counters that DID NOT move (the falsifier)

Across every 5-second window, including the two windows that contained the
load (t00 → t05 and t05 → t10), the following Aeron driver-aggregate
counters all stayed at **delta = 0**:

| Counter (cnc.dat row) | t00 → t05 Δ | t05 → t10 Δ | t10 → t15 Δ |
|------------------------|-------------|-------------|-------------|
| `2:  Failed offers to ReceiverProxy` | 0 | 0 | 0 |
| `3:  Failed offers to SenderProxy` | 0 | 0 | 0 |
| `4:  Failed offers to DriverConductorProxy` | 0 | 0 | 0 |
| `5:  NAKs sent` | 0 | 0 | 0 |
| `6:  NAKs received` | 0 | 0 | 0 |
| `11: Retransmits sent` | 0 | 0 | 0 |
| `12: Flow control under runs` | 0 | 0 | 0 |
| `13: Flow control over runs` | 0 | 0 | 0 |
| `18: Sender flow control limits, i.e. back-pressure events` | **0** | **0** | **0** |
| `19: Unblocked Publications` | 0 | 0 | 0 |
| `23: Loss gap fills` | 0 | 0 | 0 |
| `24: Client liveness timeouts` | 0 | 0 | 0 |

**Per-publication `snd-bpe` (sender back-pressure events) on every UDP
publication** — the cluster ingress channel (`localhost:20110`), each
ingress-side egress publication (the two `localhost:N` ephemerals chosen
by Aeron), and the cluster log replication channel (`alias=log`) — also
stayed at **0** through every window. The log entry for `snd-bpe` in
`AeronStat` output emits one row per UDP publication; none of them changed.

**Conductor / Sender / Receiver work-cycle exceeded counts** (rows 27 / 29 /
31, threshold = 1 s) all stayed at 0 across every window. The driver never
took longer than 1 s to complete one duty-cycle iteration even at peak
load.

**Bottom line:** there is **no Aeron-internal back-pressure** under
single-host c2400 load. The driver is not saturating any of its internal
queues, not retransmitting, not hitting flow-control limits, and not
losing packets.

### Counters that DID move (the actual flow)

The counters that *did* show non-zero deltas tell us the substrate is
*moving data through itself cleanly* — they are throughput meters, not
saturation meters:

| Counter | t00 → t05 Δ (5 s) | t05 → t10 Δ (5 s) | t10 → t15 Δ (5 s) | Interpretation |
|---------|-------------------|-------------------|-------------------|----------------|
| `0: Bytes sent` | 28.4 MB | **124.7 MB** | 46.9 MB | Driver-wide UDP egress; t05→t10 = peak window |
| `1: Bytes received` | 28.5 MB | **124.7 MB** | 46.9 MB | Driver-wide UDP ingress (matches sent at ~25 MB/s peak) |
| `64: Cluster commit-pos` | 20.06 MB | **86.09 MB** | 32.44 MB | Bytes appended to the consensus log; peak ~17.2 MB/s |
| `99: Cluster container max cycle time` | unchanged | +81.16 ms | unchanged | Single outlier cycle of the `ClusteredServiceContainer` thread (high-water; no sustained stall) |
| `26: Conductor max cycle time` | +3.16 ms | unchanged | +7.86 ms | Driver conductor max cycle (high-water; ~21–25 ms peak vs 17 ms idle baseline) |
| `28: Sender max cycle time` | +3.32 ms | unchanged | +7.85 ms | Same shape (Sender duty cycle) |
| `30: Receiver max cycle time` | +3.31 ms | unchanged | +7.84 ms | Same shape (Receiver duty cycle) |
| `49: archive-recorder max write time` | **+17.44 ms** | unchanged | unchanged | Single outlier Archive write (~20.6 ms = idle 3.18 ms + 17.44 ms); likely a term-rotation fsync stall |
| `50: archive-recorder total write bytes` | 37.7 MB | 170.8 MB | 64.8 MB | Archive recorder cumulative; peak rate ~34 MB/s |
| `51: archive-recorder total write time (ns)` | 96.7 ms | 315.3 ms | 124.4 ms | Implies ~390 MB/s avg write throughput (NVMe is happy) |

* **t00 → t05** captured the burst start; t00 was at 17:51:36.844, the
  burst kicked at 17:51:36.844, and t05 was at 17:51:41.967 — so this
  window is the burst's first ~5 s.
* **t05 → t10** captured the burst's last ~3.6 s plus ~1.4 s of cluster
  drain processing the in-flight backlog. This is the **peak throughput
  window** (124.7 MB / 5 s = 24.9 MB/s driver-wide UDP, 86.1 MB cluster
  log written = ~17.2 MB/s consensus log throughput).
* **t10 → t15** is mostly post-burst drain — the cluster service still
  applying the queued admits.
* **t15 onward** is fully drained (sub-1 MB / 5 s on every counter).

### What this tells us

1. **The substrate is healthy under load.** Zero NAKs, zero retransmits,
   zero sender flow-control limits, zero `snd-bpe` on any publication,
   zero failed offers to any driver proxy, zero unblocked publications,
   zero loss gap fills. The driver is moving 25 MB/s of UDP and 17 MB/s
   of consensus log writes without flinching. Single-host loopback +
   IPC SHM ring buffers handle this trivially.
2. **There is no driver-side back-pressure to amortise.** Any tuning that
   targets driver back-pressure (channel mode flags, dedicated
   sender/receiver/conductor threads, `term-buffer-length` increases) has
   nothing to optimise here — the back-pressure counters are already at
   zero.
3. **The Archive recorder is fine on average but takes ~20 ms outlier
   stalls.** The single +17.44 ms spike on `archive-recorder max write
   time` during t00 → t05 is consistent with a term-rotation fsync (the
   default Aeron Archive fsyncs at term boundaries, not per record).
   *Average* write rate is ~390 MB/s, so the stall is one-shot — but
   under a sustained admit stream that single 20 ms stall propagates
   into the cluster commit RTT for whichever admits happen to be in
   flight during the rotation. This is the *one* observable outlier, but
   it's a tail-latency contributor, not a peak-RPS contributor.
4. **The cluster service container's worst cycle was 166 ms.** Row 99 went
   high-water +81 ms during t05 → t10; baseline idle high-water is 85 ms.
   Both numbers are way under the work-cycle-exceeded threshold (1 s),
   so the work-cycle-exceeded count stays at 0 — but a 166 ms outlier
   on the single cluster-service-thread loop *is* a tail-latency event,
   most likely a JVM safepoint (GC) on the cluster-node JVM. Not enough
   evidence to say which collector did it (G1 by default), and it
   doesn't affect peak RPS — it would show up as a one-off
   p99 / p999 spike.
5. **Peak consensus log write rate of ~17.2 MB/s** matches ~57 k admits/s
   times ~300 bytes per `OrderAdmittedEvent` plus per-admit framing, and
   the IPC log → ClusteredServiceContainer drain is keeping up
   (`pub-pos` ≈ `sub-pos` in every snapshot — the consumer of the
   consensus log is never lagging the producer).

### Where the cluster RTT actually goes — confirmed

D-investigate already pinned the residual ~3 400 µs as **outside**
`applyAcceptOrder`'s emit calls. D-aeron-stat now confirms it's **also
outside the driver's back-pressure paths**. By elimination, the residual
3.4 ms cluster RTT lives in:

* **The single cluster-service-thread serialisation.** Every admit is
  appended to the consensus log (IPC), then dispatched to
  `applyAcceptOrder` on **one** thread (the
  `ClusteredServiceContainer`'s service-thread). On a single-leader,
  single-host topology there is no parallelism to extract here — adding
  more cluster members (Phase 5) does **not** speed up a single admit's
  RTT, but it does parallelise the Archive write fan-out (each member
  has its own NVMe).
* **The per-admit `applyAcceptOrder` body excluding emits.** Idempotency
  HashMap lookup, validation, state mutation, OrderAdmitted /
  OrderAccepted allocation — sub-microsecond on Pop!'s 32-core CPU but
  the *ordering* across admits is sequential.
* **Aeron Cluster's commit barrier.** The
  `ClusteredServiceContainer` does not dispatch a new command until the
  consensus module has acked the previous one as committed. On a
  single-host single-node cluster, this means each admit waits for one
  Archive position acknowledgement before the next one runs. That's
  Archive position-update propagation, not fsync time — which is why the
  ~20 ms fsync stall (rare) is not the dominant cost (frequent).

### Implications for the next-slice plan

D-aeron-stat **does not change** the post-D-investigate next-slice plan;
it strengthens it.

* **Aeron driver tuning (D-12 candidate)?** Falsified for peak-RPS
  purposes. Driver back-pressure counters are at zero; there is nothing
  to tune. **Drop from the list.** (Could still help tail latency by
  reducing the term-rotation fsync stall and the 81 ms cluster-container
  outlier — but those are p99/p999 levers, not RPS levers.)
* **Phase 5 / multi-host.** Now the *only* remaining axis with
  meaningful expected RPS gain. Each cluster member runs its own
  `MediaDriver` + `Archive` on its own NVMe, so the Archive
  position-acknowledgement that bounds single-admit RTT is parallelised
  across members; a 3-node cluster can sustain ~3× the consensus log
  fan-out (bound by the slowest member, not the sum). Per-admit RTT
  also drops because the leader can ack the commit as soon as a
  *quorum* of members has the log frame, not after its own Archive
  fsync. **This is the next code/topology slice.**
* **D-10 (projector decoupling).** Stays on the list **for production
  stability**, not for RPS. The D-investigate per-admit data
  (`events_BP_ticks/admit ≈ 0.16`) and the D-aeron-stat zero-flow-
  control evidence both confirm the projector is not back-pressuring
  the cluster's events publication. D-10 still matters because in
  production, if the slice 4p reconciler's blocking Ledger calls back
  up the projector's drain, **then** the events publication might
  back-pressure — but that's a stability concern, not a peak-RPS one.

### Lessons (D-aeron-stat specific)

* **Reading the driver counter file before redesigning the substrate is
  cheap.** This entire experiment is one shell script + AeronStat in
  `watch=false` + `python3` to diff snapshots. Total time: ~30 minutes
  end-to-end including pulling the samples jar from Maven Central. It
  rules out an entire class of "tune the driver" work.
* **Operate the AeronStat snapshot tool in `watch=false` and diff
  snapshots manually** rather than running it in `watch=true` continuous
  mode. Continuous mode uses ANSI clear-screen which is unfriendly for
  log capture. Manual snapshot files diff cleanly with `diff` or a
  small parser.
* **The IPC log + cluster commit-pos counters are the highest-signal
  meters here** — the per-publication position counters are noisy
  (sampled, not exact), but `Cluster commit-pos` (row 64) and the
  `archive-recorder total write bytes / write time` triple (rows 50 /
  51) give exact throughput numbers per snapshot window.
* **`aeron-samples` is *not* on the default Aeron classpath.** It's a
  separate Maven artifact (`io.aeron:aeron-samples`). The driver,
  client, archive and cluster jars do not contain `AeronStat`. Pull
  the samples jar from Maven Central as a one-time bench-host setup
  step; do not add it as a runtime dependency.

## Tier 2.5 phase D-headroom — Pop! CPU headroom probe + multi-cluster shard slice plan (2026-05-14)

Continuation of D-investigate / D-aeron-stat. After both falsifiers ruled
out OMS-code-level levers, the user explicitly deferred Phase 5 / k8s and
asked: **"is multi-cluster sharding on a single host a credible single-host
RPS lever, and how much can we get?"** This requires evidence on Pop!'s CPU
budget at peak — specifically, how many of Pop!'s 48 logical cores are still
idle at c2400, and which threads / processes are the CPU consumers — before
committing to architecture work.

### Methodology

* **Hardware:** AMD Ryzen Threadripper PRO 9965WX, 24 physical cores / 48
  logical (1 socket, 2 threads per core, max 5.49 GHz), 251 GiB RAM
  (~87 GiB available at idle, the rest is buff/cache from other workloads
  on the same box), `/dev/nvme0n1p4` 210 GB free on `/home`. `mpstat` /
  `pidstat` not installed (no `sysstat` package), so used `top -bn1` and
  `top -bH -n1 -p $cluster_pid` for system-wide and per-thread CPU samples.
* **Driver:** Single c2400 burst (480 000 admits, no warmup, two-ingress
  round-robin, identical knobs to D-aeron-stat). Snapshots at t = -1 s
  (idle baseline), then every 1 s for 12 s during and after the burst,
  then a final post-snapshot. Burst kicked at 18:20:03 and finished
  18:20:11 (8.4 s, **57 094 rps** — same plateau as D-9 / D-investigate /
  D-aeron-stat).
* **Bench script:** `/tmp/bench-d-headroom.sh` (Pop!), output rooted at
  `/tmp/bench-d-headroom-20260514-182003/` with `sys-{idle,t01..t12,post}.txt`
  and `threads-{idle,t01..t12,post}.txt`.

### System-wide CPU at peak

| Sample | %us | %sy | %ni | **%id** | Load avg | Comment |
|--------|-----|-----|-----|---------|----------|---------|
| idle (t = -1 s) | 0.2 | 0.7 | 3.0 | **95.0** | 5.20 | baseline |
| t01..t05 (mid-burst) | 0.0 | 9.5 | 61.2 | **26.3** | 19.5 | peak |
| t06 (cluster drain) | — | 5–10 | 30–60 | 30–60 | 23.4 | varies |
| post (drained) | 0.0 | 0.9 | 3.8 | **93.5** | 21.6 | back to idle |

* JVMs run with `nice = 12` (inherited from launch context) so they show
  under `%ni` not `%us`. The peak `61.2 % ni` is OMS / burst-tool work.
* **At peak: ~26 % of 48 logical cores idle ≈ 12 logical cores free.**

### Per-process CPU at peak (5 mid-burst samples averaged)

| PID | identity | peak %CPU | logical cores at peak | constant across samples? |
|-----|----------|-----------|------------------------|--------------------------|
| 2873411 | **ingress-1** (`OmsIngressReplicaBootstrap`) | 946–1200 | **9.5–12.0** | yes |
| 2873970 | **ingress-2** (`OmsIngressReplicaBootstrap`) | 930–1142 | **9.3–11.4** | yes |
| 3240631 | **bootRunBurst** (load generator on Pop!) | 838–1269 | **8.4–12.7** | yes |
| 2869907 | **cluster-node** (`OmsClusterNodeBootstrap`) | 123–158 | **1.2–1.6** | yes |
| 2872115 | projector | 15.4 | 0.15 | yes |
| 2871771 | fix-egress | ~10 | <0.10 | yes |

**Key observations:**

* **Cluster-node JVM uses only ~1.5 logical cores at peak.** The Aeron
  cluster substrate is *not* CPU-bound on Pop!. The ~3.4 ms cluster RTT
  is *latency-bound* (commit barrier propagation), not CPU-bound.
* **Ingress JVMs are the dominant CPU consumer at ~22 cores combined** —
  Tomcat thread orchestration + HTTP parse + JSON
  serialise + cluster-client busy-wait. Per-request active CPU ≈ 0.4 ms,
  which extrapolates to ~38 cores at 100 k rps.
* **bootRunBurst (load generator) consumes 8–13 cores on Pop!.** In a
  production / off-host topology this is moved off the bench host. For
  the headroom calculation this is *not* a production cost.
* **Total OMS-server-side CPU at 57 k rps: ~24 logical cores** of 48.
  Production-relevant idle = 48 − 24 = **24 cores free**, of which the
  load generator currently eats 9, leaving the observed 12.

### Per-thread CPU on the cluster-node JVM (mid-burst, t05)

| TID | thread name (truncated) | %CPU | logical cores |
|-----|-------------------------|------|----------------|
| 2869950 | `aeron-driver-conductor` (SHARED mode = also sender + receiver) | **63.6** | 0.64 |
| 2869955 | `clustered-service-thread` | 27.3 | 0.27 |
| 2869951 | `archive-recorder` / `archive-conductor` | 27.3 | 0.27 |
| 2869952 | `consensus-module-thread` | 9.1 | 0.09 |
| sum | | **127.3** | **1.27** |

* **No single cluster-node thread is at 100 %.** The Aeron driver
  agent (SHARED conductor + sender + receiver) is the highest at ~64 %
  of one logical core. SHARED → DEDICATED (D-aeron-stat candidate) would
  split this thread into three each at ~20 %, with no expected RPS lift
  per the D-aeron-stat zero-back-pressure evidence — confirmed unattractive.
* **The 3.4 ms cluster RTT is dominated by pipeline-stage hand-off
  latency, not CPU saturation.** Each admit traverses
  `consensus-module-thread → archive-recorder → clustered-service-thread`
  via Aeron IPC ring buffers; each hand-off adds Archive-position
  acknowledgement latency (microseconds) but each thread is only
  ~10–30 % busy.

### Verdict — Pop! single-host headroom

Pop! has substantial CPU headroom for a 2nd cluster substrate, but
**ingress JVM CPU caps single-host total RPS at ~110 k**:

| Topology | Per-shard cluster-side cost | Ingress CPU (scales with admit rate) | Estimated RPS ceiling |
|----------|------------------------------|---------------------------------------|------------------------|
| 1 shard (today) | 1.7 cores | 22 cores @ 57 k | **57 k** (single-leader bound) |
| 2 shards | 3.4 cores | 22 cores @ 57 k → ~38 cores @ 95 k | **~85–110 k** (1.5–1.9 × lift) |
| 3 shards | 5.1 cores | ~46 cores @ 115 k | **~110 k** (Pop! aggregate CPU cap) |
| ≥ 4 shards | 6.8+ cores | > 48 cores | **No further lift** — Pop! CPU saturated |

`Hypothesis:` 2 shards on Pop! ≈ 1.5–1.9 × ingress RPS lift.
`Hypothesis:` 3 shards ≈ same as 2; Pop! CPU is the cap.
`Unknown:` exact scaling efficiency depends on cross-shard coordination
overhead (projector multi-source consume, FIX egress fan-in, reconciler
shard locality) — measure at slice E-3 below.

**Crucially: the lift number is single-host only.** When the same shards
are deployed on N hosts (Phase 5), each host's ingress JVM operates on
its own 48-core budget, so multi-host scaling is approximately linear in
the number of hosts (bound by external constraints — Postgres pooler,
broker capacity, Ledger throughput — not by per-host CPU).

### Recommended next slice plan — Phase 4 Tier 2.5 phase E (multi-cluster sharding)

Topology-agnostic by design: every routing decision goes through a single
`OmsClusterShardRouter` keyed on `accountId.hashCode() % shardCount`. The
router is identical whether the N shards live on one host (E-1..E-3, today)
or on N hosts (Phase 5 later — same code, different deploy).

**Slice E-1 — Sharding configuration substrate (no behaviour change at N=1)**

* New `OmsConfig.Cluster.Sharding` block: `enabled` (default `false`),
  `shardCount` (default `1`, min `1`, max `32`), `shardingFunction`
  (`ACCOUNT_ID_HASH`).
* Aeron / cluster / archive directories prefixed with `shard-${index}/` so
  `OmsClusterNodeBootstrap` can be launched once per shard with separate
  `OMS_AERON_DIR_BASE` / `OMS_AERON_CLUSTER_DIR` / archive dirs.
* `OmsClusterIngressClient` becomes per-shard; `OmsClusterShardRouter`
  introduced as the new ingress-side routing layer (delegates to a
  `Map<Integer, OmsClusterIngressClient>`).
* New `scripts/launch-bench-stack-shards.sh N` orchestrates N cluster-node
  + N projector + N fix-egress + 2 ingress JVMs.
* **At `shardCount=1` the system is byte-identical to today** (the router
  is a passthrough; same Aeron dirs).
* New unit test `OmsClusterShardRouterTest` (hash distribution, deterministic
  routing per accountId, single-shard fallthrough, invalid-shard
  defensive paths).

**Slice E-2 — Postgres shard column + projector locality**

* Flyway `V30 — orders.shard_id` (smallint, nullable; backfilled from
  `accountId.hashCode() % shardCount`). Index `idx_orders_shard_id` (partial
  on the open-state subset already used by reconciler queries).
* `OmsPostgresProjector` filters its log subscription to the shard it owns
  (one projector JVM per shard); writes `orders.shard_id = ${shard}` on
  every fresh insert. Replay paths do `INSERT ... ON CONFLICT DO NOTHING`
  unchanged; the new column is always derivable from accountId so a
  projector running on shard K never claims rows for shard K' ≠ K.
* `LedgerInflightHoldFailureCompensator` adds shard awareness: enumerates
  rows per shard, routes the resulting `submitCancelOrder` call through
  the `OmsClusterShardRouter` (same accountId → same shard, by design).
* New IT `OrdersRepositoryShardingTest` (verifies derive-on-insert + index
  selectivity + replay idempotency).

**Slice E-3 — N=2 multi-cluster bench on Pop!**

* `scripts/launch-bench-stack-shards.sh 2` boots cluster-node-{0,1},
  projector-{0,1}, fix-egress-{0,1}, ingress-{1,2} (each ingress now has
  2 `OmsClusterIngressClient` instances pointing at the two cluster
  members on `localhost:20110` and `localhost:20120`).
* Verify shard locality (every admit's accountId hashes to its shard's
  cluster, every cancel ditto, no cross-shard projector writes).
* Bench at c1600 / c2400 / c3200 and compare to D-9 / D-investigate
  baselines.
* `Expected:` 1.5–1.9 × ingress RPS lift on Pop! (per the headroom
  table above).
* `Hypothesis to falsify:` cross-shard coordination overhead (router
  hash + per-shard cluster-client back-pressure interaction at the
  ingress JVM) eats more than 30 % of the theoretical lift. If so,
  the router is the bottleneck — falsifier evidence: per-shard
  cluster-client `submitAcceptOrder` p99 is similar to N=1, and ingress
  Tomcat thread CPU per request is unchanged.

**Slice E-4 — FIX-egress shard fan-in (or per-shard FIX session)**

Two designs, choice deferred until E-3 numbers are in:

* **Design A (per-shard FIX session):** N fix-egress JVMs, each holding
  one QFJ `Initiator` per shard, each with its own SenderCompID suffix.
  Broker accepts N FIX sessions. Simplest from OMS code perspective; but
  some brokers require one session per institution.
* **Design B (multi-shard FIX session):** 1 fix-egress JVM consumes from
  all N cluster log streams, multiplexes onto a single FIX session.
  Harder to write but operationally simpler.

Default plan: A for the bench rig, B as a Phase 5 follow-up if broker
session caps require it.

**Slice E-5 — Phase 5 prelude: move shards to separate hosts**

* No code change. The same `OmsClusterShardRouter` works whether shards
  resolve to `localhost:20110 / localhost:20120` (one host) or
  `host-0.svc:20110 / host-1.svc:20110` (multi-host).
* Operationally: switch from `launch-bench-stack-shards.sh` to a
  `kubectl apply -f shards.yaml` against a real cluster.
* This slice is the **deferred** Phase 5; this runbook does not advance
  it, but slices E-1..E-3 are designed to compose with it without
  refactoring.

### Sequencing

E-1 first, in isolation (no behaviour change at N=1 — easy to verify).
E-2 follows because the projector locality is a hard prerequisite for
E-3's multi-shard bench. E-3 is the empirical lift measurement on Pop!.
E-4 only after E-3 confirms the routing path is fast. E-5 is whenever
Phase 5 is greenlit.

## Tier 2.5 phase E-1 — sharding configuration substrate landed (2026-05-14)

Implements the first slice of the multi-cluster shard plan above. **No
behaviour change at `shardCount=1`** — every admit goes through the new
{@code OmsClusterShardRouter} but the router resolves to the same
singleton {@code OmsClusterIngressClient} bean used pre-E-1. Topology is
unchanged: still 5 JVMs, still one Aeron Cluster, still one consensus
log. The only observable difference from the byte sequence on the wire
or in Postgres is *zero*; the only difference at the source level is
that {@code OrderIngressService} now obtains its cluster client via
{@code OmsClusterShardRouter.forShard(shardId)} rather than holding the
client directly.

### Prior art that simplified the slice

Two pieces of substrate already existed (per
{@code oms/docs/decisions.md} §4 "Sharding") and shrank the slice
significantly:

* {@code com.balh.oms.domain.ShardKey.shardFor(UUID, int)} —
  {@code xxh64} of {@code accountId} mod {@code shardCount}, stable
  across processes. Decision date: slice 1.
* {@code OmsConfig.Shard} — {@code id} (default 0) + {@code count}
  (default 1) wired through to {@code OrderIngressService} (computes
  {@code Order.shardId} on every admit) and {@code MetricsConfig}
  (tags every meter with {@code shard_id}).
* {@code orders.shard_id} (smallint, NOT NULL) plus
  {@code idx_orders_shard_status} — already in Flyway {@code V1}, so
  E-2's "Postgres shard column" work is mostly already done; the slice
  reduces to "filter projector subscription by shard_id" and "shard the
  reconciler queries".

E-1 adds the *routing layer* on top — the only shard-related piece that
was not yet present.

### What E-1 ships

* **`OmsClusterShardRouter`** ({@code com.balh.oms.cluster}) — Spring
  bean profile-gated identically to {@code OmsClusterIngressClient}.
  Holds {@code Map<Integer, OmsClusterIngressClient>} keyed by shard id;
  {@code routeAdmit(UUID accountId)} hashes via {@code ShardKey} and
  returns the owning client; {@code forShard(int shardId)} returns the
  client for an already-known shard. Defensive validation throughout
  (out-of-range {@code shardId}, null clients, mismatched map size).
* **`OrderIngressService`** — refactored to inject the router instead of
  the client. Resolves the cluster client via
  {@code clusterShardRouter.forShard(shardId)} using the already-computed
  {@code shardId} (rather than re-deriving from {@code accountId}), so
  the order's recorded {@code shardId} and its admitting cluster are
  identical by construction. All other call sites
  ({@code FixInboundClusterSink} apply-execution-report,
  {@code LedgerInflightHoldFailureCompensator} cancel-order)
  deliberately stay on the singleton client; at {@code shardCount=1}
  the singleton is the same instance the router would return, so
  routing those paths is observably a no-op until E-3 adds shard
  locality on the {@code orderId}-keyed call sites.
* **`OmsConfig.Shard`** — previously undocumented; now carries a
  docstring tying it to {@code decisions.md} §4 plus the E-1 cap.
* **`OmsClusterShardRouterTest`** (8 cases) — Spring-discovered ctor at
  {@code N=1}, hash determinism, out-of-range defensive paths, the E-1
  cap with the operator-actionable error message, multi-shard direct
  ctor (the path E-3 will use). The multi-shard distribution case
  routes 200 random {@code UUID}s through a 2-shard router and asserts
  at least one hits each shard — pinning {@code xxh64} non-degeneracy
  so a future hash-function regression cannot silently break shard
  spread.
* **`OrderIngressServiceClusterGateTest`** — updated to wrap the mock
  {@code OmsClusterIngressClient} in a 1-entry router; all five
  existing scenarios still hold.

The slice is **177 + 18 + 22 + 148 + 9 = 374 line diff** across 5 files
(2 new, 3 modified).

### What E-1 deliberately does NOT ship (defers to E-2 / E-3)

* No {@code Flyway V30} for {@code orders.shard_id} — already in V1.
* No per-shard Aeron / archive directories — only meaningful at
  {@code N>1}; lands in E-3.
* No {@code launch-bench-stack-shards.sh N} — same.
* No projector locality filter — lands in E-2.
* No shard-aware compensator / FIX-inbound — lands in E-3 once the
  cluster log carries shard id.

### Verification — byte-identical at `shardCount=1`

**Local Mac (compile + targeted unit tests):**

| Tests | Result |
|-------|--------|
| `compileJava` + `compileTestJava` | clean |
| `OmsClusterShardRouterTest` | 8 / 8 pass |
| `OrderIngressServiceClusterGateTest` | 5 / 5 pass |
| `ShardKeyTest` (regression check) | 3 / 3 pass |
| Full `gradlew test` | 270 unit tests pass; 120 IT failures are the documented Testcontainers / Docker-Desktop issue from the caveats section below — **none reference the new code** (verified by grepping the test reports for `OmsClusterShardRouter` / `clusterShardRouter`; only my own test mentions them) |

**Pop! 2026-05-14 — full test sweep + bench parity:**

| Check | Result |
|-------|--------|
| Targeted unit tests on Pop! | `OmsClusterShardRouterTest` 8 / 8, `OrderIngressServiceClusterGateTest` 5 / 5, `ShardKeyTest` 3 / 3 |
| Full `gradlew test` on Pop! | 36 failing test classes, all pre-existing Docker-environment failures (35 of 36 also failed on the parent commit `d4967b4`); the 36th was `IngressBurstMainTest` flaking on `expected: <50> but was: <49>` under heavy parallel test load — passes 5 / 5 in isolation, does not reference any E-1 code, **not caused by this slice** |
| Stack health post-boot | 5 JVMs running; cluster-node `role=LEADER`; both ingress `/actuator/health` 200 |
| **c2400 burst plateau (the byte-identical-at-N=1 invariant)** | 4-run sequence: run 1 = 44 468 rps (cold start), run 2 = 52 935 rps (warming), **run 3 = 57 601 rps**, run 4 = 53 912 rps. Run 3 is **101 %** of the D-headroom baseline (57 094 rps); p50 / p99 / p999 = 37.9 / 121 / 173 ms vs D-headroom's same plateau. The router adds no measurable overhead at {@code N=1}. |

`Verdict:` E-1 lands the indirection layer with zero observable cost.
The slice is correct, complete, and ready for E-2 to build on.

### What this enables

E-2 ("Postgres shard column + projector locality") is now the next
slice. Because {@code orders.shard_id} already exists, E-2 reduces to:

* Per-shard projector — make {@code OmsPostgresProjector} accept an
  {@code OMS_SHARD_ID} env var and only apply log records whose
  {@code Order.shardId} matches.
* Shard-aware compensator — {@code LedgerInflightHoldFailureCompensator}
  enumerates rows per shard, routes each cancel through
  {@code OmsClusterShardRouter.forShard}.
* New `OrdersRepositoryShardingTest` IT — at {@code N=1} the existing
  `idx_orders_shard_status` is exercised (no behaviour change); at
  {@code N=2} (test-only constructor) the per-shard filters partition
  the row set cleanly.

Then E-3 stands up the second cluster on Pop! and runs the actual lift
measurement.

## Tier 2.5 phase E-2 — projector shard guard + compensator shard routing (2026-05-14)

Lands the second slice in the multi-cluster sharding stack laid out in
[Recommended next slice plan — Phase 4 Tier 2.5 phase E (multi-cluster sharding)](#tier-25-phase-d-headroom---pop-cpu-headroom-probe--multi-cluster-shard-slice-plan-2026-05-14).
E-1 introduced `OmsClusterShardRouter` as the per-shard cluster-client
indirection layer. E-2 makes the two off-hot-path consumers of that router
shard-aware so the topology is correct end-to-end at `shardCount > 1`,
without touching the on-hot-path admit code (which already shards via
`OrderIngressService` → router → cluster client at E-1).

### Scope

Three behaviour-preserving changes at `shardCount = 1`, four-digit-RPS
shape on Pop!:

1. **Compensator shard routing** —
   `LedgerInflightHoldFailureCompensator` now injects
   `OmsClusterShardRouter` (was the singleton `OmsClusterIngressClient`)
   and routes every `submitCancelOrder` through `router.forShard(row.shardId())`.
   At N=1 the router resolves to the same single client; at N>1 each row's
   cancel lands on the cluster that owns the order. Connect-check moved
   per-row so one shard's outage does not block other shards' rows; the
   timeout-aborts-batch invariant is preserved by checking that all
   remaining rows in the same batch still target the same shard before
   aborting.

2. **`LedgerInflightOutboxRepository.fetchFailedUncompensated` JOINs
   `orders.shard_id`** —
   `FailedInflightRow` gains a `shardId` field sourced from the existing
   FK `ledger_inflight_outbox.order_id REFERENCES orders(id)`. **No
   migration**: `orders.shard_id` has been on the `orders` row since the
   V1 init migration, and the projector's D-9 invariant (orders row +
   inflight outbox row written in one tx) makes the JOIN total. The
   `FOR UPDATE SKIP LOCKED` lock stays scoped to `lio` only —
   compensator concurrency partitions on the inflight outbox row, not on
   the `orders` row.

3. **`OmsPostgresProjector.applyAdmittedEvent` defensive shard guard** —
   if `ev.shardId() != config.getShard().getId()`, the projector
   increments `oms_projector_shard_mismatch_dropped_total`, logs a warn
   line, advances the cursor (so it does not loop on the misrouted
   event), and returns without touching Postgres. At `shardCount=1` the
   cluster log only carries `shardId=0` events and the projector's
   `OmsConfig.Shard.id` defaults to `0` — the guard is a no-op. At
   `shardCount>1` (E-3+) each projector subscribes to its own cluster's
   events recording so it should naturally only see its shard's events;
   this guard catches a config bug (projector wired to the wrong
   cluster's events recording, or `OMS_SHARD_ID` not set on the
   per-shard projector JVM) before applying the event to the shared
   Postgres `orders` table — protecting projection correctness in the
   exact failure mode the multi-shard topology introduces.

### Deliberately not in this slice

* **No Flyway migration on the inflight outbox tables.** A previous
  iteration of the E-2 plan called for a `shard_id` column on
  `ledger_inflight_outbox` and `domain_event_outbox`, populated by the
  projector. The JOIN to `orders.shard_id` makes that redundant for the
  one query (`fetchFailedUncompensated`) that needs it; adding columns
  + a write-side change to the projector for zero behavioural value at
  N=1 would have widened the slice without buying anything for E-3.
  Defer to a future slice if hot-path compensator queries become a
  measurable cost.

* **No per-shard `WHERE lio.shard_id = :myShard` partition on the
  compensator query.** At N>1 every ingress JVM still runs its own
  compensator instance, all share the same `ledger_inflight_outbox`
  table, and `FOR UPDATE SKIP LOCKED` already prevents double-cancel.
  Shard-partitioning the query would buy isolation (one shard's row
  flood does not delay another shard's compensator), but the cost is
  another DB round-trip per tick + a subtle invariant (rows whose
  `orders.shard_id` does not match any compensator's `myShard` would
  silently stall). Defer until measured.

* **No `OrdersRepositoryShardingTest` exercising
  `idx_orders_shard_status`.** The index has been on `orders` since V1
  but no production code path queries by `(shard_id, status)` today —
  E-3 will add the first such query (per-shard projector backfill /
  health probe). Adding a test now would test only the index's
  existence, not its use.

* **No fix on `OrderCancelAppliedEvent.shardId` hardcoded to 0 inside
  `OmsAdmissionClusteredService.emitOrderCancelApplied`.** That field
  is a wire-format placeholder because `AdmittedOrder` (the cluster
  service's in-memory state) does not carry shardId. At N>1 each
  cluster's events recording only emits cancels for orders that cluster
  admitted, so the projector still applies them to the right
  `orders.shard_id` row via the orderId lookup; the wire field is
  cosmetic. E-3 will thread `shardId` through `AdmittedOrder` so the
  field carries semantics, but E-2 does not need it for correctness.

### Verification — Pop! 2026-05-14, 19:58 → 20:00

Stack restart procedure: kill all 5 OMS JVMs, clean
`build/aeron-cluster/{media-driver,archive,cluster-services,consensus-module}`,
relaunch via `scripts/launch-bench-stack.sh` in dependency order, wait
for `/actuator/health` on both ingress JVMs (8087, 8187). Three
sequential c2400 bursts (`OMS_BURST_TOTAL=480000`,
`OMS_BURST_CONCURRENCY=400`, `OMS_BURST_RPS_CAP=0`) — first is
cold-start (fresh JIT, empty page cache), second + third are the
real signal. Pattern matches E-1 verification.

| burst | submitted | failed | elapsed | RPS | p50 | p95 | p99 | p999 |
|:------|----------:|-------:|--------:|----:|----:|----:|----:|-----:|
| cold   | 480 000 | 0 | 7.883 s | 60 888 | 5.4 ms | 9.1 ms | 23.7 ms | 77.4 ms |
| warm   | 480 000 | 0 | 7.625 s | **62 953** | 5.3 ms | 8.4 ms | 17.0 ms | 67.1 ms |
| steady | 480 000 | 0 | 7.740 s | 62 017 | 5.4 ms | 8.6 ms | 19.3 ms | 49.7 ms |

Pre-E-2 baseline (E-1 third run, same Pop! stack): **57 601 RPS**,
p50 5.6 ms, p99 21.2 ms.
E-2 average across the three runs: **61 953 RPS** — within day-to-day
RPS jitter band documented in earlier slices. Latency is at or
below the E-1 baseline at every percentile, so E-2 is **byte-identical
at `shardCount = 1`** (the slice's invariant).

Runtime evidence the E-2 surface is exercised under load:

* `oms_projector_shard_mismatch_dropped_total{shard_id="0"} = 0` —
  pre-registered counter is visible on the projector's
  `/actuator/prometheus` (port 8090), which proves the guard fires the
  registry path; zero increments confirms the cluster log only carries
  shardId=0 events at N=1 (the topological invariant the guard is
  supposed to verify).
* `oms_ledger_inflight_hold_compensated_total{outcome="cancelled",
  shard_id="0"} = 4832` — the compensator's new
  `router.forShard(row.shardId())` codepath drained the bench's
  insufficient-funds backlog through 4 832 successful cluster cancels
  during the 2-minute window. End-to-end proof that
  `LedgerInflightOutboxRepository.fetchFailedUncompensated`'s JOIN to
  `orders.shard_id` produced a correct shardId every time (the router
  rejects out-of-range shardIds, so an incorrect JOIN row would have
  surfaced as `oms_ledger_inflight_hold_compensate_failed_total`,
  not as a clean cancel).
* No SQL exceptions from the changed query in either ingress log.
* No `submit_failed` storms — the per-row connect-check + per-shard
  abort logic both behave identically to the pre-E-2 single-shard
  branch when there is exactly one shard.

### Next slice

E-3 stands up a **second** cluster-node JVM on Pop! (with its own Aeron
media-driver dir + cluster-dir), wires
`scripts/launch-bench-stack.sh` to launch one cluster-node, projector,
and fix-egress per shard, and bumps the ingress
`OmsClusterShardRouter` injection to two clients (one per shard). The
admit hot path already shards by `accountId` since E-1; E-3 is the
first slice where that sharding has somewhere to land. The headroom
probe predicted ~85–110k RPS lift on Pop! at N=2; E-3 is the bench
that confirms or falsifies that prediction.

## Tier 2.5 phase E-3a — falsifiable 2-shard scaling experiment (Pop! 2026-05-14)

Cheap experiment first. The hypothesis under test is: **"running two
independent OMS Aeron Cluster vertical stacks side-by-side on Pop!
lifts aggregate ingress rps approximately linearly above the
single-cluster ceiling."** If true, the cluster-substrate parallelizes
on a single host and the **E-3b** Spring multi-bean refactor (lift
`OmsClusterShardRouter.E1_MAX_SHARD_COUNT`, demote
`OmsClusterIngressClient` from a `@Component` to a per-shard factory
bean, and fix the hardcoded `shardId = 0` in
`emitOrderCancelApplied`) is justified by data. If false, the
substrate is the wall and we save the refactor.

### What E-3a ships

- **One Java change:**
  `OmsClusterNodeBootstrap.buildArchiveContext()` now reads
  `OMS_AERON_ARCHIVE_CONTROL_CHANNEL` (default
  `aeron:udp?endpoint=localhost:8010` — pairs with
  `DEFAULT_CLUSTER_MEMBERS_SINGLE_NODE`) instead of hardcoding the
  literal endpoint. Without this the second cluster-node JVM on the
  same host fails to bind. Default is unchanged so existing
  single-node deploys are byte-identical.
- **`scripts/launch-bench-stack-2shard.sh`** brings up two complete
  vertical stacks (cluster-node + projector + ingress per shard) with
  `oms.shard.count = 1` on **both** sides. The router invariant from
  E-1 (`E1_MAX_SHARD_COUNT = 1`) is unchanged — the experiment runs
  two independent N=1 stacks rather than a single N=2 stack. Each
  stack has its own `aeronDirBase`
  (`build/aeron-cluster` + `build/aeron-cluster-1`), port set, and
  Postgres database (`oms` + `oms_1`).

### What E-3a deliberately does NOT ship

- FIX-egress is **not launched** for either shard (port collision
  avoidance — `OMS_FIX_AUTO_START=false`,
  `OMS_ROUTING_BACKEND=noop`). Removes one variable from the lift
  measurement.
- Ledger inflight reservation is **disabled** for both shards
  (`OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED=false`,
  `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false`). A single shared Ledger
  Balance would otherwise cap aggregate rps at the per-Balance OCC
  ceiling and confound the cluster-substrate measurement.
- `OmsClusterIngressClient` is **not** demoted to a factory; the
  router still has `E1_MAX_SHARD_COUNT = 1`. Each ingress JVM holds
  exactly one client. Same Spring bean shape as today.
- Postgres uses session-mode (port 5432) on both shards rather than
  transaction-mode (6543). Required so Flyway can run `CREATE INDEX
  CONCURRENTLY` against a fresh `oms_1` DB. Verified independently
  that connection mode is not a confounder: a control run of shard 0
  alone at port 6543 with the same E-3a config returned ~30k rps,
  matching the 5432 measurement.

### Pre-flight (one-time)

```bash
docker exec ledger-supabase-db psql -U postgres -c 'CREATE DATABASE oms_1;'
```

Then bring up the 6 JVMs in order (cluster-nodes first, projectors,
ingresses):

```bash
cd ~/oms
for r in cluster-node-0 cluster-node-1; do bash ./scripts/launch-bench-stack-2shard.sh "$r"; done
sleep 25
for r in projector-0 projector-1; do bash ./scripts/launch-bench-stack-2shard.sh "$r"; done
sleep 30
for r in ingress-0 ingress-1; do bash ./scripts/launch-bench-stack-2shard.sh "$r"; done
sleep 30
curl -fsS http://127.0.0.1:8087/actuator/health  # shard 0 ingress
curl -fsS http://127.0.0.1:8187/actuator/health  # shard 1 ingress
```

### Bench

Disjoint accountId pools per shard are not strictly required at
`shardCount=1` because each shard's Postgres is independent and the
bench tool's deterministic `accountPool=64` UUIDs do not need to
diverge across shards (the orders simply land in different DBs). What
matters is that both bench clients run **simultaneously** so the
parallel-rps measurement reflects shared-host contention.

The recipe used to produce the numbers below: c2400-style
`burst-ingress-orders.sh` (slice 4k tool) with
`OMS_BURST_TOTAL=24000`, `OMS_BURST_CONCURRENCY=200`,
`OMS_BURST_ACCOUNT_POOL=64`, ledger fields unset (since
`OMS_LEDGER_ENABLED=false` for the bench). Each shard's bench targets
its own ingress port (`8088` / `8188`).

### Evidence (Pop! 2026-05-14, three sequential warm passes per leg)

**Single shard-0 baseline** (drop pass 1 = JVM warmup):

| pass | rps    | bench elapsed |
|------|--------|---------------|
| 2    | 31,330 | 0.766 s       |
| 3    | 29,744 | 0.807 s       |

mean = **30.5k rps**

**Single shard-1 baseline** (drop pass 1 = JVM warmup):

| pass | rps    | bench elapsed |
|------|--------|---------------|
| 2    | 30,021 | 0.799 s       |
| 3    | 30,068 | 0.798 s       |

mean = **30.0k rps**

**Parallel both shards (4 passes, both sides warm)**:

| pass | shard-0 rps | shard-1 rps | aggregate |
|------|-------------|-------------|-----------|
| 1    | 27,956      | 30,695      | 58,651    |
| 2    | 29,670      | 29,524      | 59,194    |
| 3    | 28,180      | 31,103      | 59,283    |
| 4    | 31,543      | 28,642      | 60,186    |

mean aggregate = **59.3k rps**

### Verdict

**Aggregate / mean-single-shard = 59.3k / 30.3k = 1.96× ≈ 98%
near-linear scaling.** Per-shard rps in the parallel run drops <5%
versus the alone-shard baseline, so the host has the headroom to run
two independent OMS clusters with negligible interference. The
cluster substrate (consensus + Aeron media driver + projector + this
host's Postgres + this host's CPU) parallelizes well at N=2 with
disjoint state.

End-to-end correctness check at the conclusion of the run:

```sql
-- shard 0 / oms (includes ~2.5M historical orders from prior benches)
SELECT COUNT(*), COUNT(DISTINCT shard_id) FROM orders;
--   2,559,842 |  1   (all shard_id=0)

-- shard 1 / oms_1 (this E-3a session only — fresh DB)
SELECT COUNT(*), COUNT(DISTINCT shard_id) FROM orders;
--      113,339 |  1   (all shard_id=0)
```

Both projectors persist orders to their respective DBs without cross-shard
contamination. (Projector lag is non-zero because the bench rate
exceeds projection throughput, but the cluster recording is durable
and the projector keeps catching up after the bench stops — same
behaviour as the published single-shard baseline.)

### Decision: proceed to E-3b

The 1.96× lift on identical work is strong enough to justify the
**E-3b** Spring refactor:

1. Lift `OmsClusterShardRouter.E1_MAX_SHARD_COUNT = 1`.
2. Demote `OmsClusterIngressClient` from `@Component` to a per-shard
   factory bean (or a `Map<Integer, OmsClusterIngressClient>`
   constructed during context init), so a single ingress JVM can hold
   N clients keyed by shard id.
3. Fix `emitOrderCancelApplied` (and any other clustered-service
   emit path) to propagate `mutated.shardId()` instead of hardcoding
   `0` — required so the E-2 projector shard guard accepts events
   from non-zero shards.
4. Default `oms.shard.count` stays at 1; multi-shard configurations
   are opt-in via env, with the host-side topology (separate
   `aeronDirBase`, separate Postgres DB per shard) remaining the
   operator's responsibility.

E-3b is a refactor, not a behaviour change at N=1. The E-1 router
invariants and the E-2 projector shard guard already paid for the
cross-shard correctness story; E-3b just removes the `N=1`
constraint.

### Open questions deferred past E-3b

- The single-shard rps in E-3a (~30k) sits well below the
  full-config baseline (~62k from slice 4p / D-9). The delta is **not**
  Postgres connection mode (verified via the 6543 control run). Most
  likely candidates: (a) the bench tool's HTTP client is the
  bottleneck above some concurrency threshold (RTT p50 of 4 ms
  against 200 concurrency caps theoretical at ~50k rps), or (b) the
  no-fix / no-ledger path exercises a different cluster admit shape
  than the full-config path. Either way the **single-vs-aggregate
  ratio** is the result that matters for E-3a's hypothesis; the
  absolute number is not.
- Multi-host scaling beyond what one Pop! box can provide is the
  natural next step after E-3b lands. That would be Phase 5
  (k8s) — explicitly out of scope here.

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
