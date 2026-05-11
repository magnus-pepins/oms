# OMS

[![OMS CI](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml/badge.svg)](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml)

Order Management System for the Balh financial platform ecosystem.

This is the slice-1 bootstrap. It is a runnable Spring Boot 3 application
backed by Postgres + Flyway, with the Postgres-first / Chronicle-after
control-plane pattern wired end-to-end (outbox reconciler → Chronicle append →
tail reader → `ControlTailer` CAS updates).

**CI:** GitHub Actions runs `./gradlew clean test` against a **job-level Postgres service** (see `services:` in `.github/workflows/ci.yml` and env `OMS_CI_JDBC_*` in `AbstractPostgresIntegrationTest`), then `./gradlew bootJar` on pushes and PRs to `main` / `master`, and can be triggered manually (`workflow_dispatch`). Testcontainers is still used on CI for other images (e.g. NATS). Failed runs upload an **`oms-test-reports`** artifact (JUnit XML + HTML under `build/`). If the job log is not visible, download that artifact or run `./gradlew clean test --no-daemon --no-build-cache` locally (**Docker required** for Testcontainers Postgres and NATS-backed ITs).

The product / architecture decisions that shaped this code live in the
[system-documentation](../system-documentation) workspace — primarily
[plans/oms-fix-gateway-and-settlement.md](../system-documentation/plans/oms-fix-gateway-and-settlement.md)
and the milestone plan it links to. **Phase 1 exit (UAT soak, §16 #3, prod session store):** fill
[plans/oms-phase1-exit-actions.md](../system-documentation/plans/oms-phase1-exit-actions.md).

## What is in slice 1

- `POST /internal/v1/orders` and `GET /internal/v1/orders/{id}` (HTTP, internal
  API key auth).
- Idempotent inserts on `(account_id, client_idempotency_key)`.
- `domain_event_outbox` table; canonical JSON **envelopes** are written in the
  same Postgres transaction as the originating change (`OrderAccepted` on
  ingress; `OrderWorking` / `OrderRejected` in `ControlTailer` after CAS).
- `ledger_inflight_outbox` table (Flyway **V4**): optional **post-commit** path for
  the Ledger BUY sync inflight hold when `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true`
  (default **false** — synchronous `POST /transactions` in the accept transaction
  remains the default). `LedgerInflightOutboxReconciler` drains pending rows after
  commit; Micrometer **`oms_ledger_inflight_hold`** records `path=sync` vs
  `path=outbox`, plus counters **`oms_ledger_inflight_outbox_published_total`** /
  **`oms_ledger_inflight_outbox_failed_total`**. Tuning: `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_*`.
- `DomainFanoutReconciler` + `FanoutClient` drain `domain_event_outbox` to NATS
  JetStream (full envelope as message body) or a no-op transport when NATS is off.
- `OutboxReconciler` drains `control_outbox` into Chronicle Queue (engineering
  replay only, NOT a regulatory system of record).
- `ControlTailer` applies CAS updates on `orders.version` and, on terminal
  reject paths, records **`OrderRejected`** in the domain outbox; after a
  successful transition to **`WORKING`**, records **`OrderWorking`**.
- Hashed-account-id PII policy enforced by a Micrometer filter and a guard
  test.
- Optional **Ledger** HTTP client: when `OMS_LEDGER_ENABLED=true`, ingress verifies
  `ledgerIdentityId` against Ledger for every `ledgerBalanceId`, and `ControlTailer`
  can reject BUY orders (with `ledgerBalanceId` + `limitPrice`) for insufficient
  `availableBalance` (`RISK_BUYING_POWER`) before CAS to `WORKING`.
- Optional **NATS JetStream** fanout: when `OMS_NATS_ENABLED=true`, `NatsFanoutClient`
  publishes envelope JSON after the outbox reconciler sees committed rows.
- Three operational drill scripts (failover, broken-chronicle,
  reconciler-under-load) — skeletons that document assertions; bodies grow with
  later slices.

## What is in slice 2 (risk + audit)

- Flyway **V5** — `control_decisions` (one row per `ControlTailer.apply` outcome)
  and `oms_runtime_flags` (interim Postgres-backed **global halt** until Redis/Ops
  toggles exist).
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

## What is in slice 3 (return path — simulated broker)

- Flyway **V6** — `executions` (idempotent on `(account_id, venue_exec_ref)`),
  `market_context` (stub JSON merged with venue-attested fields on each trade apply), `orders.cum_filled_quantity`.
- **`ExecutionReportApplier`** — applies trade and cancel ER-shaped commands;
  emits **`OrderPartiallyFilled`**, **`OrderFilled`**, **`OrderCancelled`** to
  `domain_event_outbox`; metrics **`oms_executions_applied_total`**,
  **`oms_order_filled_events_published_total`**.
- **`RouteDispatcher`** — `ControlTailer` registers **after-commit** enqueue when
  CAS reaches **`WORKING`** (no-op when `OMS_ROUTING_BACKEND=noop`).
- **`SimulatedBrokerDispatcher`** + **`SimulatedReturnPathProjectionWorker`** + **`SimulatedExecutionProgram`**
  when `OMS_ROUTING_BACKEND=simulated`, drains a queue and fills in three chunks at `limit_price`.
  **`FixRouteDispatcher`** + QuickFIX/J initiator/outbound/inbound when `OMS_ROUTING_BACKEND=fix` (slice 4; see [docs/return-path.md](docs/return-path.md) and [docs/fix-out.md](docs/fix-out.md)).

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

**JDK 21 + Chronicle:** `build.gradle.kts` passes OpenHFT’s `--add-opens` / `--add-exports` to **`bootRun`** and **`test`** (see `chronicleJavaModuleOpens`). If you run the fat JAR with plain `java -jar`, pass the same flags (or set `JAVA_TOOL_OPTIONS`) or Chronicle will fail opening the queue on Linux.

### Compose Postgres vs integration tests

The Postgres from `docker compose up -d postgres` is what **`bootRun`**
connects to when you set `OMS_PG_URL` as above.

**Locally,** `./gradlew test` integration tests use **Testcontainers**, which starts a
**separate** short-lived Postgres container on an ephemeral port. That only
requires the Docker daemon (same machine as Gradle); it does not read
`OMS_PG_URL`. Having Compose Postgres up is fine — both can run at once.

**On GitHub Actions CI,** the workflow starts a **service** Postgres container instead;
tests point at it via `OMS_CI_JDBC_URL` (see `AbstractPostgresIntegrationTest`).

`BuyingPowerLedgerControlTailerIntegrationTest` also starts an embedded
**WireMock** Ledger stub (no separate Ledger container).

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

- [docs/architecture.md](docs/architecture.md) — Postgres-first flow,
  sequence diagrams, HA stance.
- [docs/decisions.md](docs/decisions.md) — slice-1 locked decisions
  (idempotency, sharding, time policy, etc.).
- [docs/pii-policy.md](docs/pii-policy.md) — what may appear where.
- [docs/replay.md](docs/replay.md) — Chronicle's role and how to replay.
- [docs/chronicle-tail-driver.md](docs/chronicle-tail-driver.md) — `scheduled` vs `dedicated` tail wake / latency tuning.
- [docs/drop-copy-events.md](docs/drop-copy-events.md) — domain-event
  envelope and emit points.
- [docs/return-path.md](docs/return-path.md) — executions, simulated routing, idempotency, slice-6 fill→positions.
- [docs/settlement.md](docs/settlement.md) — custody, `positions`, broker confirm queue, §12.3 transitions, internal `/internal/v1/settlement/**` (executions list/detail, **manual-actions** stage/list/approve).
- [docs/fix-out.md](docs/fix-out.md) — FIX slice 4 wiring status.
- [docs/fix-outbound-driver.md](docs/fix-outbound-driver.md) — `scheduled` vs `dedicated` FIX outbound wake / latency tuning.
- [docs/configuration.md](docs/configuration.md) — every named limit /
  env key in one place.
