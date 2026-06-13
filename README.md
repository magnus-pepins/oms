# OMS

[![OMS CI](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml/badge.svg)](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml)

Order Management System for the Balh financial platform ecosystem.

**Platform documentation:** [system-documentation/systems/oms.md](../system-documentation/systems/oms.md) — ports, PM2 roles, pop scripts, cross-system integration.

A runnable Spring Boot 3 application backed by Postgres + Flyway. Phase 3 of the
[Aeron Cluster substrate plan](../system-documentation/plans/oms-aeron-cluster-substrate.md)
is the source of truth for OMS topology: order admission and execution-report
state run inside `OmsAdmissionClusteredService` on the cluster nodes, the
`oms-postgres-projector` JVM consumes the cluster events recording and writes
projection rows (`orders`, `executions`, `control_decisions`,
`domain_event_outbox`, market context, positions), and the `oms-fix-egress`
JVM owns QuickFIX `SocketInitiator` for outbound NOS and inbound execution
reports.

**Role JVMs (one Spring profile per role; `TopologyWorkerProfiles` enforces mutual exclusion):**

- **`oms-cluster-node`** — `./gradlew bootRunClusterNode` / `bootJarClusterNode`. Runs MediaDriver + Archive + ConsensusModule + ClusteredServiceContainer hosting `OmsAdmissionClusteredService`. StatefulSet, ≥3 replicas per shard.
- **`oms-postgres-projector`** — `./gradlew bootRunPostgresProjector` / `bootJarPostgresProjector`. Subscribes to the cluster events recording via Aeron Archive replay and projects to Postgres. Idempotent on replay.
- **`oms-fix-egress`** — `./gradlew bootRunFixEgress` / `bootJarFixEgress`. Singleton FIX initiator per route; reads the events recording for outbound NOS and offers `ApplyExecutionReportCommand` back to the cluster on inbound venue ER. Runbook: [`docs/runbooks/oms-fix-egress.md`](docs/runbooks/oms-fix-egress.md) (TODO).
- **`oms-ingress-replica`** — `./gradlew bootRunIngressReplica` / `bootJarIngressReplica`. Horizontal HTTP/gRPC ingress; submits `AcceptOrderCommand` through `OmsClusterIngressClient`. Runbook: [`docs/runbooks/oms-ingress-replica.md`](docs/runbooks/oms-ingress-replica.md).

**CI:** GitHub Actions runs `./gradlew clean test` against a **job-level Postgres service** (see `services:` in `.github/workflows/ci.yml` and env `OMS_CI_JDBC_*` in `AbstractPostgresIntegrationTest`), then **`./gradlew bootJar bootJarIngressReplica bootJarClusterNode bootJarPostgresProjector bootJarFixEgress`** on pushes and PRs to `main` / `master`, and can be triggered manually (`workflow_dispatch`). Testcontainers is still used on CI for other images (e.g. NATS). Failed runs upload an **`oms-test-reports`** artifact (JUnit XML + HTML under `build/`). If the job log is not visible, download that artifact or run `./gradlew clean test --no-daemon --no-build-cache` locally (**Docker required** for Testcontainers Postgres and NATS-backed ITs).

The product / architecture decisions that shaped this code live in the
[system-documentation](../system-documentation) workspace — primarily
[plans/oms-fix-gateway-and-settlement.md](../system-documentation/plans/oms-fix-gateway-and-settlement.md)
and the milestone plan it links to. **Phase 1 exit (UAT soak, §16 #3, prod session store):** fill
[plans/oms-phase1-exit-actions.md](../system-documentation/plans/oms-phase1-exit-actions.md).

## Slice 1 — order ingress + admission

- `POST /internal/v1/orders` and `GET /internal/v1/orders/{id}` (HTTP, internal
  API key auth; **not registered** on `oms-postgres-projector` or `oms-fix-egress` — **registered** on the monolith JVM and on `oms-ingress-replica`).
- Idempotent inserts on `(account_id, client_idempotency_key)`.
- `domain_event_outbox` table; canonical JSON **envelopes** are written in the
  same Postgres transaction as the originating change. `OrderAccepted` is
  written by `OrderIngressService` on the ingress JVM. `OrderWorking` /
  `OrderRejected` are written by the projector inside the same transaction
  that applies the cluster's `OrderAdmittedEvent` to Postgres.
- `ledger_inflight_outbox` table (Flyway **V4**): optional **post-commit** path for
  the Ledger BUY sync inflight hold when `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`
  (default **false** — synchronous `POST /transactions` in the accept transaction
  remains the default). `LedgerInflightOutboxReconciler` drains pending rows after
  commit; Micrometer **`oms_ledger_inflight_hold`** records `path=sync` vs
  `path=outbox`, plus counters **`oms_ledger_inflight_outbox_published_total`** /
  **`oms_ledger_inflight_outbox_failed_total`**. Tuning: `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_*`.
- `DomainFanoutReconciler` + `FanoutClient` drain `domain_event_outbox` to NATS
  JetStream (full envelope as message body) or a no-op transport when NATS is off.
- `OmsAdmissionClusteredService` (cluster) walks risk + buying-power + admission
  state machine; `OmsPostgresProjector` (`oms-postgres-projector` JVM) projects
  the resulting `OrderAdmittedEvent` to `orders` (CAS to `WORKING` /
  `REJECTED`), `control_decisions`, and `domain_event_outbox` in one
  transaction. `OrderControlAdmission` is the shared admission helper invoked
  by the projector.
- Hashed-account-id PII policy enforced by a Micrometer filter and a guard
  test.
- Optional **Ledger** HTTP client: when `OMS_LEDGER_ENABLED=true`, ingress verifies
  `ledgerIdentityId` against Ledger for every `ledgerBalanceId`, and the cluster
  admission service can reject BUY orders (with `ledgerBalanceId` + `limitPrice`)
  for insufficient `availableBalance` (`RISK_BUYING_POWER`) before promoting to
  `WORKING`.
- Optional **NATS JetStream** fanout: when `OMS_NATS_ENABLED=true`, `NatsFanoutClient`
  publishes envelope JSON after the outbox reconciler sees committed rows.
- Operational drill scripts (failover, reconciler-under-load) — skeletons that
  document assertions; bodies grow with later slices.

## What is in slice 2 (risk + audit)

- Flyway **V5** — `control_decisions` (one row per admission outcome — projected
  by `OmsPostgresProjector` from `OrderAdmittedEvent`) and `oms_runtime_flags`
  (interim Postgres-backed **global halt** until Redis/Ops toggles exist).
- **Internal HTTP** — `GET` / `PATCH /internal/v1/runtime-flags/global_halt` (same
  `X-OMS-Internal-Key` gate as other `/internal/v1/**` routes) to read/update the
  halt flag for Ops Console proxies.
- **`GET /internal/v1/control-decisions`** — paginated read on `control_decisions` (`orderId` and/or `from`/`to`, `limit`/`offset`) for Ops Console audit UI.
- **`ControlRiskEvaluator`** — `RISK_KILL_SWITCH` when `global_halt` is true;
  optional instrument allowlist; fat-finger limit price / quantity; notional cap
  (`RISK_NOTIONAL_CAP` on `reject_code`). See [docs/risk-checks.md](docs/risk-checks.md).
- **`oms_control_jobs_rejected_stale_total`** counter on stale control rejects;
  default **`OMS_CONTROL_MAX_JOB_AGE_MS`** is **300000** (5 min interim — see
  [system-documentation/plans/oms-phase0-interim-decisions.md](../system-documentation/plans/oms-phase0-interim-decisions.md)).

## What is in slice 3 (return path)

- Flyway **V6** — `executions` (idempotent on `(account_id, venue_exec_ref)`),
  `market_context` (stub JSON merged with venue-attested fields on each trade apply), `orders.cum_filled_quantity`.
- **Cluster path (Phase 3 of the Aeron Cluster substrate plan):** inbound venue
  ER from QuickFIX is translated by `FixInboundClusterSink` (`oms-fix-egress`
  JVM) into `ApplyExecutionReportCommand` and offered to the cluster.
  `OmsAdmissionClusteredService` walks the order state machine deterministically
  and emits `ExecutionAppliedEvent`. `OmsPostgresProjector` then writes the
  `executions` row, the `orders` CAS, the `market_context` merge, the
  `positions` / `position_history` deltas, the optional free-riding attribution
  links, and the `domain_event_outbox` envelope (`OrderPartiallyFilled`,
  `OrderFilled`, `OrderCancelled`, `OrderRejected`) in one transaction.
  Metrics: `oms_executions_applied_total`, `oms_order_filled_events_published_total`.
- **Slice 5 prep:** FIX outbound **`oms.fix.symbol-map-json`**; optional **tradability** list → **`RISK_INSTRUMENT_NOT_ALLOWED`** ([docs/fix-out.md](docs/fix-out.md), [docs/risk-checks.md](docs/risk-checks.md)).

## What is NOT in slice 1

- FIX **production** hardening (named broker UAT sign-off, multi-day soak) — see [oms-realignment-2026-05-07.md](../system-documentation/plans/oms-realignment-2026-05-07.md), [docs/fix-broker-uat-soak.md](docs/fix-broker-uat-soak.md), and the **scheduling / sign-off runbook** [plans/oms-phase1-exit-actions.md](../system-documentation/plans/oms-phase1-exit-actions.md). **Optional** second Postgres for JDBC session store: Compose profile **`with-fix-session-db`** + [docs/fix-session-store-isolation.md](docs/fix-session-store-isolation.md).
- Full risk catalogue from the master plan (STP, sanctions re-check, venue-specific
  checks, …) — only the slice‑2 subset above is implemented so far.
- Cluster-aware lease ownership (slice 1.5).

## Quick start

```bash
# 1. Postgres (and optionally NATS JetStream for local fanout)
docker compose up -d postgres
# docker compose --profile with-nats up -d   # then OMS_NATS_ENABLED=true OMS_NATS_URL=nats://localhost:4222
# Optional second Postgres for QuickFIX JdbcStore only (see docs/fix-session-store-isolation.md):
# docker compose --profile with-fix-session-db up -d postgres-fix-sessions

# 2. Build
./gradlew build

# 3. Run (uses the Compose Postgres on localhost:5432 — defaults match docker-compose.yml)
OMS_INTERNAL_API_KEY=local-key \
OMS_PG_URL=jdbc:postgresql://localhost:5432/oms \
OMS_PG_USER=oms \
OMS_PG_PASSWORD=oms \
./gradlew bootRun
```

Flyway migrations run on startup against that database.

**Listen port:** default **8088** (`OMS_HTTP_PORT`) so OMS does not bind **8080** (often used by the marketing website on the same dev host). Override with `OMS_HTTP_PORT` if needed.

**JDK 21 + Aeron:** `build.gradle.kts` passes the Aeron / Agrona `--add-opens` / `--add-exports` flags to `bootRun*` and `test` (see `lowLatencyJvmModuleOpens`). If you run a fat JAR (cluster-node, postgres-projector, fix-egress, ingress-replica) with plain `java -jar`, pass the same flags (or set `JAVA_TOOL_OPTIONS`) or Aeron will fail to open `Unsafe` / off-heap buffers on Linux.

### Compose Postgres vs integration tests

The Postgres from `docker compose up -d postgres` is what **`bootRun`**
connects to when you set `OMS_PG_URL` as above.

**Locally,** `./gradlew test` integration tests use **Testcontainers**, which starts a
**separate** short-lived Postgres container on an ephemeral port. That only
requires the Docker daemon (same machine as Gradle); it does not read
`OMS_PG_URL`. Having Compose Postgres up is fine — both can run at once.

**On GitHub Actions CI,** the workflow starts a **service** Postgres container instead;
tests point at it via `OMS_CI_JDBC_URL` (see `AbstractPostgresIntegrationTest`).

Buying-power integration tests start an embedded **WireMock** Ledger stub
(no separate Ledger container).

```bash
curl -s http://localhost:8088/internal/v1/orders \
  -H "X-OMS-Internal-Key: local-key" \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "00000000-0000-0000-0000-000000000001",
    "clientIdempotencyKey": "demo-1",
    "side": "BUY",
    "instrumentSymbol": "AAPL",
    "quantity": "10",
    "limitPrice": "150.00",
    "timeInForce": "DAY",
    "ledgerBalanceId": "balance_replace_with_real_id",
    "ledgerIdentityId": "ledger_identity_that_owns_that_balance"
  }'
```

## Tests

```bash
./gradlew test
```

Integration tests extend `AbstractPostgresIntegrationTest`, which uses **Testcontainers Postgres** when `OMS_CI_JDBC_URL` is **not** set (local / IDE). When that env var **is** set (GitHub Actions CI), JDBC targets that URL instead. If Docker is not running and you are not using CI env, those classes are **skipped** (see
`@Testcontainers(disabledWithoutDocker = true)` on
`AbstractPostgresIntegrationTest`) so `./gradlew build` still succeeds.
`ShardKeyTest` always runs without Docker.

To exercise the full integration suite, start Docker Desktop (or the Docker
daemon) and run `./gradlew test` again — you should see the Postgres-backed
tests execute instead of being skipped.

## Documentation

- [docs/architecture.md](docs/architecture.md) — order admission flow,
  cluster topology, projector / FIX-egress wiring.
- [docs/decisions.md](docs/decisions.md) — slice-1 locked decisions
  (idempotency, sharding, time policy, etc.).
- [docs/pii-policy.md](docs/pii-policy.md) — what may appear where.
- [docs/replay.md](docs/replay.md) — Aeron Archive recording / replay.
- [docs/drop-copy-events.md](docs/drop-copy-events.md) — domain-event
  envelope and emit points.
- [docs/return-path.md](docs/return-path.md) — cluster `ExecutionAppliedEvent` → projector → executions / orders / market_context / positions / domain outbox.
- [docs/settlement.md](docs/settlement.md) — custody, `positions`, broker confirm queue, §12.3 transitions, internal `/internal/v1/settlement/**` (executions list/detail, **manual-actions** stage/list/approve).
- [docs/fix-out.md](docs/fix-out.md) — FIX slice 4 wiring status.
- [docs/fix-outbound-driver.md](docs/fix-outbound-driver.md) — `scheduled` vs `dedicated` FIX outbound wake / latency tuning.
- [docs/configuration.md](docs/configuration.md) — every named limit /
  env key in one place.
