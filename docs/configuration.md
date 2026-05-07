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
| `OMS_CONTROL_MAX_JOB_AGE_MS`       | `5000`  | If a control event sits unprocessed past this, reject the order with `RISK_STALE_QUEUE`.             |
| `OMS_TAILER_BATCH_SIZE`            | `100`   | Reserved for future control-plane batching (Disruptor slice); Chronicle tail batch uses `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES`. |

## Outbox / reconciler

| Key                                   | Default | Meaning                                                                            |
|---------------------------------------|---------|------------------------------------------------------------------------------------|
| `OMS_OUTBOX_RECONCILER_AGE_MS`        | `2000`  | Minimum age before an outbox row is eligible for Chronicle append (avoids races).  |
| `OMS_OUTBOX_RECONCILER_BATCH_SIZE`    | `100`   | Rows fetched per reconciler tick.                                                  |
| `OMS_OUTBOX_RECONCILER_INTERVAL_MS`   | `500`   | Scheduled-task interval. Drives `oms_control_outbox_appended_total` cadence.       |

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
| `OMS_NATS_ENABLED`           | `false`                | When true, the NATS-backed publisher displaces the no-op (slice 1.5+).    |
| `OMS_NATS_URL`               | `nats://localhost:4222`| NATS connection URL.                                                      |
| `OMS_NATS_SUBJECT_PREFIX`    | `oms.events`           | Subject prefix; events publish to `${prefix}.${type}`.                    |

## PII

| Key                              | Default              | Meaning                                                                                  |
|----------------------------------|----------------------|------------------------------------------------------------------------------------------|
| `OMS_PII_AUDIT_TRACE_ENABLED`    | `false`              | When true, audit-grade trace logs MAY include raw `account_id`. Off in dev/prod.         |
| `OMS_PII_HASH_SECRET`            | `dev-only-change-me` | Seeds `xxh64` for `account_id` hashing. Must be rotated and managed in deploy tooling.   |
