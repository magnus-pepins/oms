# Chronicle control tail driver

OMS reads the **control** Chronicle queue in a second step after Postgres commit: `OutboxReconciler` appends excerpts; `ChronicleControlTailReader` calls `ControlTailer.apply` (CAS on `orders.version`). How often the JVM **wakes** to call `ExcerptTailer.readBytes` is configurable.

### Turning off the tail on an ingress-only JVM (`oms.chronicle.control-tail-enabled`)

When **`oms.chronicle.control-tail-enabled=false`** (env **`OMS_CHRONICLE_CONTROL_TAIL_ENABLED`**), Spring does **not** register **`ChronicleControlTailReader`** or the scheduled **`pollBatch`** task. Chronicle **queue** and **append** beans still load when **`oms.chronicle.enabled=true`**, so ingress-after-commit or reconciler append keeps working. Use on **ingress-only** replicas so they do not share the same `control-tail-id` cursor as control/FIX workers. The **`oms-ingress-replica`** Spring profile wires these defaults via **`application-oms-ingress-replica.yaml`** (see [runbooks/oms-ingress-replica.md](../runbooks/oms-ingress-replica.md)).

If **order accept** runs on that JVM, pair with **`oms.control.postgres-write-path=ingress`**; **`IngressChronicleTailTopologyValidator`** rejects **`postgres-write-path=tail`** with the tail off. **`oms-control-worker`** / **`oms-fix-worker`** require **`control-tail-enabled=true`**.

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

## Shared queue directory and tailer identity

`ChronicleControlTailReader` calls `queue.createTailer(id)` with **`oms.chronicle.control-tail-id`** / **`OMS_CHRONICLE_CONTROL_TAIL_ID`** (default **`oms-control`**). Chronicle persists tailer progress under **`oms.chronicle.queue-dir`**.

- **Production default:** treat **`OMS_CHRONICLE_QUEUE_DIR`** as **one logical journal** and run **one** JVM that tails it for control apply (often co-located with FIX on the “control” role, or a single control-worker replica). Postgres CAS makes *replay* mostly safe, but **two processes with the same tailer id on the same path** can corrupt tailer index files and cause undefined behaviour.
- **Intentional parallelism** requires an agreed model: e.g. **separate** `OMS_CHRONICLE_QUEUE_DIR` per shard with one tail each, or (advanced) distinct **`OMS_CHRONICLE_CONTROL_TAIL_ID`** values only when you accept independent cursors and your own dedup rules — not the default N-replica “scale control horizontally” story.

On each JVM start the reader calls **`toStart()`** only when **`OMS_CHRONICLE_CONTROL_TAIL_REPLAY_FROM_START_ON_BOOT`** is **`true`** (default). When **`false`**, it resumes the persisted index for **`OMS_CHRONICLE_CONTROL_TAIL_ID`**; see `ChronicleControlTailReader` and `oms.chronicle.control-tail-replay-from-start-on-boot`.

## Configuration reference

| Property / env | Default | Applies when |
|----------------|---------|----------------|
| `oms.chronicle.control-tail-id` / `OMS_CHRONICLE_CONTROL_TAIL_ID` | `oms-control` | Chronicle enabled — `ExcerptTailer` id under `queue-dir` (alphanumeric, `_`, `.`, `-`; max 128 chars). |
| `oms.chronicle.control-tail-replay-from-start-on-boot` / `OMS_CHRONICLE_CONTROL_TAIL_REPLAY_FROM_START_ON_BOOT` | `true` | When `true`, calls `toStart()` on boot (full replay for that tailer id). When `false`, resumes persisted tail index. |
| `oms.chronicle.tail-driver` / `OMS_CHRONICLE_TAIL_DRIVER` | `scheduled` | Always (selects mode). |
| `oms.chronicle.tail-poll-interval-ms` / `OMS_CHRONICLE_TAIL_POLL_INTERVAL_MS` | `50` | **`scheduled` only** — delay between `pollBatch` invocations (ms). |
| `oms.chronicle.tail-batch-max-messages` / `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES` | `200` | Both — max excerpts consumed per inner drain pass. |
| `oms.chronicle.tail-dedicated-idle-park-nanos` / `OMS_CHRONICLE_TAIL_DEDICATED_IDLE_PARK_NANOS` | `100000` (~0.1 ms) | **`dedicated` only** — `LockSupport.parkNanos` after a pass that read nothing. **`0`** = no park (busy-wait when idle; extreme profiling only). |

Startup logs a single **INFO** line with `tailerId`, `driver=SCHEDULED` (and `pollIntervalMs`) or `driver=DEDICATED` (and `idleParkNanos`, `batchMax`).

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
