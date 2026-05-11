# Chronicle control tail driver

OMS reads the **control** Chronicle queue in a second step after Postgres commit: `OutboxReconciler` appends excerpts; `ChronicleControlTailReader` calls `ControlTailer.apply` (CAS on `orders.version`). How often the JVM **wakes** to call `ExcerptTailer.readBytes` is configurable.

This is **not** a Chronicle-provided push callback into application code. The tail is always **polled** (`readBytes`). The choice is **who** invokes that poll and **how long** the thread sleeps when the queue is empty.

## Modes (`oms.chronicle.tail-driver`)

| Value | Env | Behaviour |
|--------|-----|------------|
| `scheduled` (default) | `OMS_CHRONICLE_TAIL_DRIVER=scheduled` | Spring runs `pollBatch()` on a **fixed delay** after each completion (`SchedulingConfigurer` bean `chronicleControlTailPollScheduling` in `ChronicleQueueConfiguration`). Interval: `OMS_CHRONICLE_TAIL_POLL_INTERVAL_MS`. Worst-case additional latency after an append is roughly **one poll interval** (plus scheduler jitter), before `readBytes` runs again. |
| `dedicated` | `OMS_CHRONICLE_TAIL_DRIVER=dedicated` | A **non-daemon** thread named `oms-chronicle-control-tail` loops: drain up to `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES` per inner pass; when a full pass reads nothing, park for `OMS_CHRONICLE_TAIL_DEDICATED_IDLE_PARK_NANOS`. **No** Spring scheduled task is registered for the tail. |

### Why use `dedicated`?

For **latency tuning** and benchmarks where you want to remove the Spring scheduler from the critical path between “excerpt available” and “`ControlTailer.apply` runs”. While work is arriving faster than you drain, the thread stays in the inner drain loop without waiting on the global scheduler.

### Trade-offs

- **`scheduled`**: Predictable CPU when idle; simple operational model. Tail latency includes **poll interval** when the queue was empty just before the append.
- **`dedicated`**: Lower **tail-wake** latency when combined with a small idle park (or `0` for busy-wait when idle — see below). Costs: one dedicated thread; if `OMS_CHRONICLE_TAIL_DEDICATED_IDLE_PARK_NANOS=0`, **CPU spin** while no messages (only use briefly for profiling).
- **End-to-end slice-1** latency still includes **other** scheduled components (`OutboxReconciler`, FIX outbound worker, etc.). Tuning the tail driver alone does not remove those waits; see `docs/configuration.md` (pipeline timers, outbox / FIX intervals).

## Configuration reference

| Property / env | Default | Applies when |
|----------------|---------|----------------|
| `oms.chronicle.tail-driver` / `OMS_CHRONICLE_TAIL_DRIVER` | `scheduled` | Always (selects mode). |
| `oms.chronicle.tail-poll-interval-ms` / `OMS_CHRONICLE_TAIL_POLL_INTERVAL_MS` | `50` | **`scheduled` only** — delay between `pollBatch` invocations (ms). |
| `oms.chronicle.tail-batch-max-messages` / `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES` | `200` | Both — max excerpts consumed per inner drain pass. |
| `oms.chronicle.tail-dedicated-idle-park-nanos` / `OMS_CHRONICLE_TAIL_DEDICATED_IDLE_PARK_NANOS` | `100000` (~0.1 ms) | **`dedicated` only** — `LockSupport.parkNanos` after a pass that read nothing. **`0`** = no park (busy-wait when idle; extreme profiling only). |

Startup logs a single **INFO** line with `driver=SCHEDULED` (and `pollIntervalMs`) or `driver=DEDICATED` (and `idleParkNanos`, `batchMax`).

## Shutdown

In `dedicated` mode, graceful shutdown sets a stop flag, **interrupts** the tail thread, and **joins** with a bounded timeout (`DEDICATED_THREAD_JOIN_TIMEOUT_MS` in `ChronicleControlTailReader`). If the thread is still alive after that, a **WARN** is logged.

## Related code

- `com.balh.oms.chronicle.ChronicleControlTailReader` — tail creation, `pollBatch`, dedicated loop, `DisposableBean`.
- `com.balh.oms.config.ChronicleQueueConfiguration#chronicleControlTailPollScheduling` — registers fixed-delay `pollBatch` when `tail-driver=scheduled` (bean lives in the same configuration class as the tail reader so scheduling is never skipped due to `@ConditionalOnBean` ordering).
- `com.balh.oms.chronicle.ChronicleTailDriver` — enum.
- `com.balh.oms.config.OmsConfig.Chronicle` — bound properties.

## See also

- [architecture.md](architecture.md) — control-plane sequence (tail reader participant).
- [configuration.md](configuration.md) — full env tables including Chronicle and pipeline metrics.
