# Configuration reference

Every named limit in the OMS slice 1, with defaults and meaning. Mirrors
[.env.example](../.env.example) and `application.yaml`.

Per the Balh ecosystem rule (`config-and-limits.mdc`), no bare numeric
literals appear in code for timeouts, retries, batch sizes, etc. Add a
new key here when introducing one.

## HTTP

| Key                         | Default | Meaning                                                    |
|-----------------------------|---------|------------------------------------------------------------|
| `OMS_HTTP_PORT`             | `8080`  | Bind port for the internal HTTP surface.                   |
| `OMS_INTERNAL_API_KEY`      | (empty) | Shared secret for `/internal/v1/**`. Empty rejects all.    |

## Postgres

| Key                              | Default                                  | Meaning                                  |
|----------------------------------|------------------------------------------|------------------------------------------|
| `OMS_PG_URL`                     | `jdbc:postgresql://localhost:5432/oms`   | JDBC URL.                                |
| `OMS_PG_USER`                    | `oms`                                    | DB user.                                 |
| `OMS_PG_PASSWORD`                | `oms`                                    | DB password.                             |
| `OMS_PG_POOL_MAX_SIZE`           | `20`                                     | HikariCP max pool size.                  |
| `OMS_PG_POOL_MIN_IDLE`           | `5`                                      | HikariCP min idle.                       |
| `OMS_PG_CONNECTION_TIMEOUT_MS`   | `2000`                                   | HikariCP connection timeout.             |

## Sharding

| Key                | Default | Meaning                                          |
|--------------------|---------|--------------------------------------------------|
| `OMS_SHARD_ID`     | `0`     | Logical shard id of this instance.               |
| `OMS_SHARD_COUNT`  | `1`     | Total shard count for `xxh64 mod` mapping.       |

## Control plane

| Key                                | Default | Meaning                                                                                              |
|------------------------------------|---------|------------------------------------------------------------------------------------------------------|
| `OMS_CONTROL_MAX_JOB_AGE_MS`       | `300000` | If a control event sits unprocessed past this (ms), reject with `RISK_STALE_QUEUE`. Interim default 5 min — [oms-phase0-interim-decisions.md](../../system-documentation/plans/oms-phase0-interim-decisions.md). |
| `OMS_TAILER_BATCH_SIZE`            | `100`   | Reserved for future control-plane batching (Disruptor slice); Chronicle tail batch uses `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES`. |

## Risk (slice 2)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_RISK_INSTRUMENT_ALLOWLIST_ENABLED` | `false` | When `true`, only symbols in `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` may pass control. |
| `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` | (empty) | Comma-separated list (e.g. `AAPL,MSFT`). Compared uppercased. |
| `OMS_RISK_FAT_FINGER_MAX_LIMIT_PRICE` | `0` | Max limit price per order; `0` disables. |
| `OMS_RISK_FAT_FINGER_MAX_ORDER_QUANTITY` | `0` | Max order quantity; `0` disables. |
| `OMS_RISK_MAX_ORDER_NOTIONAL` | `0` | Max `quantity × limit_price`; `0` disables. |

## Routing / return path (slice 3)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_ROUTING_BACKEND` | `noop` | `noop` — no post-`WORKING` dispatch. `simulated` — `SimulatedFillEngine` emits synthetic fills. `fix` — reserved (slice 4; fails fast at startup). |
| `OMS_MARKET_CONTEXT_STUB_JSON` | `{"stub":true}` | JSON written on first fill to `market_context.snapshot_json` until marketdata integration (slice 5). |
| `OMS_SIMULATED_VENUE_ID` | `SIM` | `venue_id` on synthetic executions. |
| `OMS_SIMULATED_QUEUE_CAPACITY` | `10000` | Bounded queue for `WORKING` order ids awaiting simulation. |
| `OMS_SIMULATED_POLL_INTERVAL_MS` | `50` | `@Scheduled` drain interval when simulated backend is enabled. |
| `OMS_SIMULATED_SCHEDULER_ENABLED` | `true` | When `false`, only explicit `SimulatedFillEngine.drainOnceForTests()` drains the queue (used in integration tests). |

## Outbox / reconciler

| Key                                   | Default | Meaning                                                                            |
|---------------------------------------|---------|------------------------------------------------------------------------------------|
| `OMS_OUTBOX_RECONCILER_AGE_MS`        | `2000`  | Minimum age before a control outbox row is eligible for Chronicle append (avoids races).  |
| `OMS_OUTBOX_RECONCILER_BATCH_SIZE`    | `100`   | Rows fetched per reconciler tick.                                                  |
| `OMS_OUTBOX_RECONCILER_INTERVAL_MS`   | `500`   | Scheduled-task interval. Drives `oms_control_outbox_appended_total` cadence.       |

## Domain fanout outbox

| Key                                      | Default | Meaning                                                                 |
|------------------------------------------|---------|-------------------------------------------------------------------------|
| `OMS_DOMAIN_EVENTS_RECONCILER_AGE_MS`    | `2000`  | Minimum age before a `domain_event_outbox` row is eligible for NATS delivery. |
| `OMS_DOMAIN_EVENTS_RECONCILER_BATCH_SIZE` | `100` | Rows fetched per `DomainFanoutReconciler` tick.                         |
| `OMS_DOMAIN_EVENTS_RECONCILER_INTERVAL_MS` | `500` | `@Scheduled` fixed delay for domain fanout drain.                       |

## Chronicle

| Key                                   | Default            | Meaning                                                                 |
|---------------------------------------|--------------------|-------------------------------------------------------------------------|
| `OMS_CHRONICLE_ENABLED`               | `true`             | When `false`, no `ChronicleQueue` bean; `NoOpControlJournal` is used.    |
| `OMS_CHRONICLE_QUEUE_DIR`             | `./queues/control` | Directory for Chronicle Queue files.                                    |
| `OMS_CHRONICLE_ROLL_CYCLE`            | `DAILY`            | `MINUTELY` / `HOURLY` / `DAILY` use `LegacyRollCycles`. Also: `FAST_DAILY`, `DEFAULT`. |
| `OMS_CHRONICLE_TAIL_POLL_INTERVAL_MS` | `50`             | Delay between scheduled `ChronicleControlTailReader` polls (ms).        |
| `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES` | `200`          | Max excerpts read per poll tick.                                        |

## NATS (fanout)

| Key                          | Default                | Meaning                                                                   |
|------------------------------|------------------------|---------------------------------------------------------------------------|
| `OMS_NATS_ENABLED`           | `false`                | When true, wires `NatsFanoutClient` (JetStream) instead of the no-op `FanoutClient`. |
| `OMS_NATS_URL`               | `nats://localhost:4222`| NATS connection URL.                                                      |
| `OMS_NATS_SUBJECT_PREFIX`    | `oms.events`           | Subject prefix; events publish to `${prefix}.${type}`.                    |
| `OMS_NATS_STREAM_NAME`       | `OMS_EVENTS`           | JetStream stream name created on startup (idempotent if it exists).       |
| `OMS_NATS_CONNECTION_TIMEOUT_MS` | `5000`            | NATS TCP connect timeout.                                                 |

## Ledger (buying power)

| Key                              | Default                 | Meaning                                                                 |
|----------------------------------|-------------------------|-------------------------------------------------------------------------|
| `OMS_LEDGER_ENABLED`             | `false`                 | When true, wires `RestLedgerBalanceClient` and enables the BUY gate in `ControlTailer`. |
| `OMS_LEDGER_BASE_URL`          | `http://localhost:5001` | Ledger HTTP root (routes mounted at `/`, e.g. `/balances/{id}`).        |
| `OMS_LEDGER_API_KEY`           | (empty)                 | Value for `X-Ledger-Key` on balance reads. Required when enabled.         |
| `OMS_LEDGER_CONNECT_TIMEOUT_MS`  | `2000`                  | HTTP connect timeout for Ledger calls.                                  |
| `OMS_LEDGER_READ_TIMEOUT_MS`     | `5000`                  | HTTP read timeout for Ledger calls.                                      |
| `OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED` | `false`          | When true, OMS places a Ledger BUY inflight hold at order accept (idempotent `reference` `oms:order:{uuid}`). Requires `OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID`. |
| `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED` | `false` | When **true** (and inflight reservation enabled), the hold is **written to `ledger_inflight_outbox` in the same Postgres transaction** as the order; `LedgerInflightOutboxReconciler` calls Ledger **after** commit. When **false**, the Ledger HTTP call runs **synchronously inside** the accept transaction (default). |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_AGE_MS` | `2000` | Only process outbox rows with `created_at` at least this old (avoids racing uncommitted readers). |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_BATCH_SIZE` | `50` | Max rows per reconciler tick. |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_INTERVAL_MS` | `500` | `fixedDelay` between reconciler runs (Spring `@Scheduled`). |
| `OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID` | (empty) | Ledger `balance_id` for the hold leg (bank suspense / OMS hold account). Required when inflight reservation is enabled. |
| `OMS_LEDGER_INFLIGHT_RESERVATION_CURRENCY` | `EUR`        | ISO currency code for the inflight hold `amount`.                        |
| `OMS_LEDGER_INFLIGHT_RESERVATION_PRECISION` | `100`       | Ledger amount scaling (e.g. 100 = cents).                                |

## PII

| Key                              | Default              | Meaning                                                                                  |
|----------------------------------|----------------------|------------------------------------------------------------------------------------------|
| `OMS_PII_AUDIT_TRACE_ENABLED`    | `false`              | When true, audit-grade trace logs MAY include raw `account_id`. Off in dev/prod.         |
| `OMS_PII_HASH_SECRET`            | `dev-only-change-me` | Seeds `xxh64` for `account_id` hashing. Must be rotated and managed in deploy tooling.   |
