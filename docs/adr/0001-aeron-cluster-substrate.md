# ADR 0001 — Aeron Cluster as the OMS journal substrate, Java as the implementation language

**Status:** accepted, in implementation (Phase 0 spike pending; see plan).

**Supersedes:** [`docs/decisions.md`](../decisions.md) §1 (Posture A — Chronicle Queue OSS) and §8 (Chronicle's role).

**Plan:** [system-documentation/.cursor/plans/finish_oms_topology_aeron_cluster_9ae3ce16.plan.md](../../../system-documentation/.cursor/plans/finish_oms_topology_aeron_cluster_9ae3ce16.plan.md).

## Context

The OMS is built around Postgres as system of record with Chronicle Queue OSS as a downstream "engineering replay" journal. The current topology plan ([`plans/oms-ingress-control-fix-topology.md`](../../../system-documentation/plans/oms-ingress-control-fix-topology.md)) ships split-deploy mechanics on the same single-substrate model. Two unsolved properties of that model:

- **Multi-reader**: Chronicle OSS gives no native multi-reader safety on a shared queue dir. The current plan worked around this with shard-id discipline; it remained a manual invariant rather than a property of the substrate.
- **Replication / HA**: Chronicle OSS has no native replication. Replicated journals (Chronicle Enterprise paid, Aeron Cluster, Kafka) were noted as an "upgrade path" but not committed.

The product trajectory now includes the option of becoming a matching engine / exchange (e.g. running an internal prediction market with institutional market-makers). That trajectory makes microsecond-class latency a real constraint and Raft-replicated HA a real requirement, neither of which Chronicle OSS satisfies.

## Decision

### Substrate: Aeron Cluster

OMS will be refactored into a clustered state-machine service running on **Aeron Cluster**. The Aeron Raft-replicated log is the source of truth. The in-memory `ClusteredService` state is reconstructed from the log on every node. **Postgres becomes a downstream projection** for read-side queries, audit, and analytics.

This replaces decision §1 (Posture A — Chronicle Queue OSS) and §8 (Chronicle as engineering replay only). Chronicle is removed from the codebase as each phase of the plan replaces it; there is no dual-substrate config.

### Language: Java

Java is retained for the OMS. C++ was considered and rejected for this rewrite.

#### Why Java is sufficient at OMS-class latencies

- LMAX Exchange (Java + Disruptor) is the existence proof at exchange-class workloads. Adaptive's clustered systems run Java on Aeron Cluster.
- The latency floor delta between well-written Java and well-written C++ at the OMS layer is **nanoseconds, not microseconds**. The OMS is bottlenecked on Postgres projection lag, network, broker FIX session, and Raft consensus — none of which language choice changes.
- Java with discipline (Agrona off-heap buffers, single-writer, no allocation in hot paths, ZGC / Shenandoah) reaches microsecond p99 on Aeron Cluster. This is the standard low-latency Java playbook.

#### Why C++ now is wrong

- 12-18 months to rewrite the existing Java/Spring Boot OMS (23 Flyway migrations, hundreds of files, HTTP/gRPC controllers, QuickFIX/J, Micrometer, Spring profiles).
- Aeron Cluster's reference implementation and most production deployments are Java. C++ cluster server exists but is less battle-tested.
- QuickFIX/J is mature; QuickFIX C++ is less actively maintained. Alternative (FIX Antenna, Onixs) is paid + vendor lock-in.
- Hiring pool for low-latency C++ is smaller / more expensive than for low-latency Java.
- Ecosystem (`customer-frontend`, `ledger`, `beard-admin`) is TypeScript/Java. Adding C++ adds polyglot operational tax.

#### Why C++ is kept open for later

- Aeron is **language-neutral**. A future C++ matching engine module can subscribe to the same Aeron streams the Java cluster emits and emit executions back. SBE generates compatible Java and C++ codecs from one schema.
- Decision deferred to **Phase 6 (matching engine)** in the plan, to be revisited when a concrete matching workload exists to benchmark.

### Wire format: SBE (intent), Agrona DirectBuffer (transitional)

We will adopt **SBE (Simple Binary Encoding)** for cluster command/event wire format because it preserves the language-neutral property described above. SBE generates Java and C++ codecs from one XML schema and is the canonical wire format for Aeron-based systems.

For the first scaffolding PRs we use hand-coded Agrona `DirectBuffer` encoding to keep the diff narrow and the build green. SBE adoption (with a Gradle SBE plugin and `src/main/resources/sbe/*.xml` schemas) lands as a follow-up before any cross-language consumer attaches.

### Discipline applied in cluster code

These rules apply inside `ClusteredService.onSessionMessage` and any deterministic apply path:

- No `Instant.now()`, `System.currentTimeMillis()`, `UUID.randomUUID()`, no `java.util.Random`. Time and ids come from the command payload (cluster client supplies them) or the cluster-supplied `timestamp` parameter.
- No external I/O (no Postgres calls, no HTTP, no FIX `sendToTarget`, no logging at INFO+ inside the hot path). External effects happen at the **edge** — pre-cluster (cluster client) or post-cluster (egress consumer).
- Off-heap memory in hot paths via Agrona (`DirectBuffer`, `Long2ObjectHashMap`, `Object2ObjectHashMap`).
- No reflection, no Spring magic, no annotations on the `ClusteredService` itself. It is a plain Java class wired by hand from the cluster bootstrap.

A determinism lint / static analysis step is on the Phase 1 backlog.

## Consequences

- **Refactor scope:** the largest change in the OMS codebase to date. Estimate 3-6 months focused work for plan Phases 1-3.
- **No fallback:** if the Phase 0 spike fails (team cannot operationalize Aeron Cluster on the target hardware / k8s topology), substrate decision is reopened from scratch. There is no Chronicle escape hatch baked into this plan.
- **Postgres semantics flip:** writes to `orders`, `executions`, `control_decisions` etc. become eventually-consistent projections from cluster egress. Callers that read Postgres immediately after a successful HTTP response see brief staleness. See plan section "Phase 1" for the explicit risk callout.
- **HA built in:** 3-node Raft cluster tolerates 1 node failure. 5-node tolerates 2. No more "Chronicle Enterprise upgrade path" caveat.
- **Operational surface:** more components than the current single-JVM OMS (3-node cluster + projector + FIX egress + cluster client + media driver). Documented per-role runbooks in Phase 5.
- **Vendor risk:** Aeron is open source. Real Logic (primary maintainer) is small but active. We can self-support; a paid support contract via Real Logic is a Phase 0 decision (currently leaning community-only).

## Alternatives considered

- **Stay on Chronicle Queue OSS**: rejected. Multi-reader and replication remain unsolved. The current topology plan's workarounds are workarounds.
- **Chronicle Enterprise (paid replication)**: rejected on cost (per-node license), vendor lock-in, and lack of native cluster consensus model.
- **Apache Kafka / Redpanda**: rejected on latency floor (millisecond, not microsecond). Acceptable for retail-only OMS; precluded by exchange roadmap.
- **NATS JetStream**: rejected on latency floor (sub-ms p50, single-digit ms p99 under load).
- **Aeron Transport + Aeron Archive (no cluster)**: rejected as a stepping stone. Doing Transport+Archive first, then upgrading to Cluster, is real work twice; the savings is small. Direct path to Cluster preferred.
- **C++ rewrite of the OMS**: rejected on cost, ecosystem fit, and absence of measurable benefit at OMS-class latencies. Reconsidered for Phase 6 (matching engine) only.

## Versions pinned at decision time

- Aeron: `1.48.0` (Maven Central, 2025-06).
- Agrona: `2.2.1` (Maven Central, 2025-06).
- Java: 21 (existing toolchain).
- Spring Boot: 3.3.x (existing).

These are starting versions. The Phase 0 spike confirms or updates them before Phase 1 PRs land.
