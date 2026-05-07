# OMS

[![OMS CI](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml/badge.svg)](https://github.com/magnus-pepins/oms/actions/workflows/ci.yml)

Order Management System for the Balh financial platform ecosystem.

This is the slice-1 bootstrap. It is a runnable Spring Boot 3 application
backed by Postgres + Flyway, with the Postgres-first / Chronicle-after
control-plane pattern wired end-to-end (outbox reconciler → Chronicle append →
tail reader → `ControlTailer` CAS updates).

**CI:** GitHub Actions runs `./gradlew test` and `./gradlew bootJar` on pushes
and PRs to `main` / `master`, and can be triggered manually (`workflow_dispatch`);
see `.github/workflows/ci.yml`.

The product / architecture decisions that shaped this code live in the
[system-documentation](../system-documentation) workspace — primarily
[plans/oms-fix-gateway-and-settlement.md](../system-documentation/plans/oms-fix-gateway-and-settlement.md)
and the milestone plan it links to.

## What is in slice 1

- `POST /internal/v1/orders` and `GET /internal/v1/orders/{id}` (HTTP, internal
  API key auth).
- Idempotent inserts on `(account_id, client_idempotency_key)`.
- `control_outbox` table; rows are written inside the same Postgres
  transaction as the `orders` row.
- `OutboxReconciler` drains the outbox into Chronicle Queue (engineering
  replay only, NOT a regulatory system of record).
- `ControlTailer` applies CAS updates on `orders.version` and, on terminal
  reject paths, publishes **`OrderRejected`**; after a successful transition to
  **`WORKING`**, publishes **`OrderWorking`** (same fire-and-forget contract as
  `OrderAccepted`).
- Hashed-account-id PII policy enforced by a Micrometer filter and a guard
  test.
- Optional **Ledger** HTTP client: when `OMS_LEDGER_ENABLED=true`, `ControlTailer`
  can reject BUY orders (with `ledgerBalanceId` + `limitPrice`) for insufficient
  `availableBalance` (`RISK_BUYING_POWER`) before CAS to `WORKING`.
- Optional **NATS JetStream** publisher: when `OMS_NATS_ENABLED=true`, domain
  events publish after Postgres commit (see `NatsDomainEventPublisher`).
- Three operational drill scripts (failover, broken-chronicle,
  reconciler-under-load) — skeletons that document assertions; bodies grow with
  later slices.

## What is NOT in slice 1

- FIX engine (slice 2: QuickFIX/J).
- Full risk catalogue (kill switch, fat-finger, venue-specific checks).
- Cluster-aware lease ownership (slice 1.5).

## Quick start

```bash
# 1. Postgres (and optionally NATS JetStream for local fanout)
docker compose up -d postgres
# docker compose --profile with-nats up -d   # then OMS_NATS_ENABLED=true OMS_NATS_URL=nats://localhost:4222

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

### Compose Postgres vs integration tests

The Postgres from `docker compose up -d postgres` is what **`bootRun`**
connects to when you set `OMS_PG_URL` as above.

`./gradlew test` integration tests use **Testcontainers**, which starts a
**separate** short-lived Postgres container on an ephemeral port. That only
requires the Docker daemon (same machine as Gradle); it does not read
`OMS_PG_URL`. Having Compose Postgres up is fine — both can run at once.
`BuyingPowerLedgerControlTailerIntegrationTest` also starts an embedded
**WireMock** Ledger stub (no separate Ledger container).

```bash
curl -s http://localhost:8080/internal/v1/orders \
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
    "ledgerBalanceId": "balance_replace_with_real_id"
  }'
```

## Tests

```bash
./gradlew test
```

Integration tests use Testcontainers (Postgres). They run when Docker is
available. If Docker is not running, those classes are **skipped** (see
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
- [docs/drop-copy-events.md](docs/drop-copy-events.md) — domain-event
  envelope and emit points.
- [docs/configuration.md](docs/configuration.md) — every named limit /
  env key in one place.
