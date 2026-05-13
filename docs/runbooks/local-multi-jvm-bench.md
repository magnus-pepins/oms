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
| Cluster-client commit round-trip (slice 4c) | `oms.cluster.client.commit_round_trip_seconds` Δ histogram | summarize_cluster_pipeline_deltas.py |
| Projector cursor lag at end of run (slice 4d) | `oms.projector.lag_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| FIX-egress cursor lag at end of run (slice 4d) | `oms.fix_egress.lag_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| Snapshot freshness (slice 4h) | `oms.cluster.snapshot.age_seconds` gauge | summarize_cluster_pipeline_deltas.py |
| Cluster-path tail latency, GC-corrected | HdrHistogram p50/p99/p99.9/max (in-process) | `./gradlew clusterBench` (slice 4e) |

**What it does NOT measure**: a per-order ingress→NOS wall-clock distribution. That single-process
OTel histogram (`oms.fix.ingress_to_nos`) was deleted in slice 4i because the cross-JVM
stop-the-clock hook was orphaned in the slice-3b-2 / 3g topology cut. The closest honest substitute
is the derived **upper bound** = `commit_round_trip_p99 + fix_egress_lag_seconds` printed at the end
of the summary; it conflates the cluster path with whatever projection / FIX session backlog exists
at scrape time, but it's the best you get without re-introducing a wire-format change to carry the
ingress start-time through cluster events.

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

Then OMS bootRun env on Pop:

```bash
export OMS_PG_URL='jdbc:postgresql://127.0.0.1:5432/oms'
export OMS_PG_USER='postgres.your-tenant-id'   # tenant prefix is REQUIRED
export OMS_PG_PASSWORD='your-super-secret-and-long-postgres-password'
```

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
./gradlew bootRunPostgresProjector
# Prometheus: http://127.0.0.1:8090/actuator/prometheus
# main HTTP:  http://127.0.0.1:8093  (auto-started by Spring Boot but not used by the role)
# applies cluster events (OrderAdmittedEvent, ExecutionAppliedEvent) to Postgres rows
```

Without this, the cluster will admit orders but `orders` / `executions` rows never materialise
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
OMS_FIX_AUTO_START=true \
OMS_FIX_SOCKET_CONNECT_PORT=9876 \
OMS_FIX_SENDER_COMP_ID=OMS_INIT \
./gradlew bootRunFixEgress
# Prometheus: http://127.0.0.1:8091/actuator/prometheus
# main HTTP:  http://127.0.0.1:8094  (defaulted in application-oms-fix-egress.yaml; not used)
# Spring property: oms.routing.backend=fix is wired in application-oms-fix-egress.yaml
```

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

Notes on the asymmetric output:

- `oms_cluster_client_commit_round_trip` (slice 4c) is registered without
  `publishPercentileHistogram(true)`, so the Prometheus scrape carries `_sum` / `_count` but
  no `_bucket{le=...}` series. The summary explicitly flags this and points at `clusterBench`,
  which records the same metric in HdrHistogram with coordinated-omission correction.
- `oms_pipeline_ingress_accept` does carry buckets, so its p50 / p99 are real (they reference
  Micrometer's bucket-edge `le` values, not interpolated quantiles — round up by one bucket
  for safety).
- `oms_cluster_snapshot_age_seconds` ≈ cluster-node uptime when no operator snapshot has
  fired during the run; not an SLO violation. Trigger a snapshot mid-run with
  `./gradlew clusterSnapshot` (see `oms-cluster-node-snapshot.md`) to exercise the gauge.

What to compare against:

- HTTP `time_total` p99 vs `oms_pipeline_ingress_accept` p99 — the gap is JSON / Spring HTTP /
  network frame overhead.
- `oms_pipeline_ingress_accept` p99 vs `oms_cluster_client_commit_round_trip` p99 — the gap is
  Postgres TX commit (`domain_event_outbox` insert + optional ledger inflight).
- `oms_projector_lag_seconds` and `oms_fix_egress_lag_seconds` against the SLO doc thresholds
  (5 s warn / 30 s critical; see [cluster-slo.md](../cluster-slo.md)).
- `oms_cluster_snapshot_age_seconds` — only meaningful if you've taken a snapshot via
  `./gradlew clusterSnapshot` during the run.

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
