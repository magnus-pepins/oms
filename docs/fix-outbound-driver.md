# FIX outbound driver (`scheduled` vs `dedicated`)

When `oms.routing.backend=fix` and `oms.fix.auto-start=true`, `FixRouteDispatcher` stores **WORKING** order ids in an **in-process** `BlockingQueue`. `FixOutboundDispatchWorker` dequeues and calls `Session.sendToTarget` for each `NewOrderSingle`.

That worker must **wake** somehow. The driver chooses between Spring scheduling and a dedicated thread (same idea as Chronicle `tail-driver`; see [chronicle-tail-driver.md](chronicle-tail-driver.md)).

## Modes (`oms.fix.outbound-driver`)

| Value | Env | Behaviour |
|--------|-----|------------|
| `scheduled` (default) | `OMS_FIX_OUTBOUND_DRIVER=scheduled` | `SchedulingConfigurer` bean `fixOutboundPollScheduling` in `FixAutoStartBeans` runs `drainPendingOutboundOnce()` on a **fixed delay** after each completion. Interval: `OMS_FIX_OUTBOUND_POLL_INTERVAL_MS`. At most **one** dequeue per tick when session is logged on and `fix_route_state.send_enabled` is true. |
| `dedicated` | `OMS_FIX_OUTBOUND_DRIVER=dedicated` | Non-daemon thread `oms-fix-outbound-dispatch`: when logon and send are enabled, **`poll(timeout)`** on the queue with timeout `OMS_FIX_OUTBOUND_DEDICATED_IDLE_PARK_NANOS` (returns immediately when an id is already queued). When not logged on or send is disabled, **parks** `OMS_FIX_OUTBOUND_DEDICATED_NOT_READY_PARK_NANOS` (if set to `0`, a **1 ms** floor applies to avoid hot-spinning). No Spring scheduled task for outbound. |

### Why use `dedicated`?

To remove **scheduler quantization** on the path from “id enqueued” to “`sendToTarget` runs”, similar to `OMS_CHRONICLE_TAIL_DRIVER=dedicated`. Useful for latency benchmarks; production trade-off is one thread and CPU when the queue is hot.

### What this does **not** do

- **Not** a QuickFIX callback: inbound FIX is already callback-driven (`fromApp`, …). Outbound is **OMS-owned** queue + worker; “dedicated” only changes **how the JVM wakes** the worker.
- **Not** multi-process: the queue is **heap-local** to one OMS JVM. Multiple OMS replicas do **not** share one outbound queue (see architecture note below).

## Configuration reference

| Property / env | Default | Meaning |
|----------------|---------|---------|
| `oms.fix.outbound-driver` / `OMS_FIX_OUTBOUND_DRIVER` | `scheduled` | `scheduled` or `dedicated`. |
| `oms.fix.outbound-poll-interval-ms` / `OMS_FIX_OUTBOUND_POLL_INTERVAL_MS` | `100` | **`scheduled` only** — delay between `drainPendingOutboundOnce` runs. |
| `oms.fix.outbound-dedicated-idle-park-nanos` / `OMS_FIX_OUTBOUND_DEDICATED_IDLE_PARK_NANOS` | `100000` | **`dedicated` only** — `BlockingQueue.poll` timeout when queue may be empty (`0` uses a **1 ns** minimum wait in code to avoid a degenerate poll storm; prefer a small positive value). |
| `oms.fix.outbound-dedicated-not-ready-park-nanos` / `OMS_FIX_OUTBOUND_DEDICATED_NOT_READY_PARK_NANOS` | `50000000` (~50 ms) | **`dedicated` only** — park when session not logged on or route send disabled (`0` → **1 ms** floor). |

Startup logs **INFO** with `FIX outbound driver=SCHEDULED` or `FIX outbound driver=DEDICATED` and the relevant tuning numbers.

## Shutdown

`dedicated` mode: `DisposableBean` sets a stop flag, **interrupts** the thread, **joins** with a bounded timeout (`DEDICATED_THREAD_JOIN_TIMEOUT_MS` in `FixOutboundDispatchWorker`). WARN if still alive.

## Multi-instance note

- **Outbound queue**: one `LinkedBlockingQueue` **per OMS process**. Many HTTP clients can POST to the **same** OMS instance; all enqueues hit **that** queue. **Several OMS instances** ⇒ **several separate queues** — not one shared queue unless you introduce an external broker (not this design).
- **Chronicle / Postgres control**: slice 1 assumes **single-writer** semantics for a shard; multiple OMS replicas against the same DB without a **shard lease / leader** are not supported for authoritative control (see [shard-lease.md](shard-lease.md)).

## Related code

- `com.balh.oms.fix.FixOutboundDispatchWorker`
- `com.balh.oms.fix.FixRouteDispatcher`
- `com.balh.oms.config.FixAutoStartBeans#fixOutboundPollScheduling`
- `com.balh.oms.fix.FixOutboundDriver`

## See also

- [fix-out.md](fix-out.md) — FIX slice wiring.
- [configuration.md](configuration.md) — full env tables.
