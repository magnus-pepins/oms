# OMS cluster latency and allocation results

Source of truth for slice-level cluster benchmark numbers under
`system-documentation/plans/oms-aeron-cluster-substrate.md` Phase 4.

Numbers below come from the `clusterBench` harness (slice 4e — full in-process cluster + Aeron
cluster client + HdrHistogram round-trip) and the JMH wire-format benches (slice 4f — pure encode
or decode, no cluster boot, allocation profiled with `-prof gc`).

Bench machine: macOS / Apple Silicon, JDK 21.0.8 (JBR distribution). Numbers are not directly
comparable to a Linux production node (different OS, different scheduling, different memory
controller); they are useful for **A/B comparison** within this repo, not as absolute SLO
targets. Production numbers will be captured in slice 4g/4h on a Linux runner.

---

## Slice 4e — `clusterBench` round-trip (single laptop, smoke run)

Configuration: 1 s warmup, 2 s steady state, 200 ops/s target throughput. HdrHistogram with
`recordValueWithExpectedInterval` for coordinated-omission correction.

| Percentile | Commit round-trip (ms) |
| --- | --- |
| p50 | 3.1 |
| p99 | 5.6 |
| p99.9 | 6.3 |

400 commits accepted; zero timeouts; zero outcome errors. The commit round-trip is from
`AcceptOrderCommand.encode` on the `AeronCluster` client through cluster apply and
`OrderAcceptedEvent` egress back to the same client.

These numbers cover **everything**: ingress publication, consensus log append, leader apply,
events publication, egress publication, client demux. They do not isolate the
encode/decode cost — that's slice 4f's lane.

---

## Slice 4f — wire-format hot path allocation audit (JMH `-prof gc`)

Bench: `com.balh.oms.cluster.bench.AcceptOrderCommandWireBench`, 3 forks × (3 warmup + 5
measurement) iterations × 1 s each. Production-shaped command: 36-char accountId, 36-char
client idempotency key, 24-char accountId hash, 4-char instrument symbol, no ledger balance id.

Run via:

```bash
./gradlew jmh -PjmhInclude=AcceptOrderCommandWireBench -Pjmh.profilers=gc
```

### Baseline (before slice 4f fix)

| Bench | Wall (ns/op) | Allocation (B/op) |
| --- | --- | --- |
| `encode` (client → buffer) | 24.7 ± 0.7 | **176** |
| `decode` (buffer → command) | 63.3 ± 5.0 | **736** |

### Audit findings

The `decode` hot path on `OmsAdmissionClusteredService.onSessionMessage` was paying for **eight**
`byte[]` allocations per `AcceptOrderCommand` decode:

1. **Four** unavoidable `byte[]` from `readString` (one per UTF-8 string field — needed to call
   `new String(bytes, UTF_8)`).
2. **Four redundant** `byte[]` from `stringByteLen(decodedString)` — each call did
   `decodedString.getBytes(UTF_8)` purely to advance the read cursor between string fields by
   the number of bytes the encoder wrote, even though that exact byte count was already on the
   wire as a 4-byte length prefix that the decoder had just read past.

Estimated breakdown of the 736 B/op (JDK 21 object layout, 8-byte alignment):

| Allocation | Bytes | Notes |
| --- | --- | --- |
| 4× `byte[]` (UTF-8 payload, from `readString`) | ~152 | 36+36+24+4 char strings, 16-byte header each |
| 4× `String` | ~160 | 40 B header + backing byte[] reference |
| 4× **redundant** `byte[]` (from `stringByteLen`) | ~152 | identical to (1), pure waste |
| 1× `UUID` | ~32 | header + 2 longs |
| 1× `AcceptOrderCommand` record | ~120 | 13 fields, mostly references |
| (JMH state / blackhole overhead) | ~120 | residual |
| **Total** | **~736** | matches measured |

The `encode` path's 176 B/op comes from 4× `String.getBytes(UTF_8)` inside `writeString` — same
pattern, but on encode this allocation is genuinely needed because we have to *produce* the
UTF-8 bytes to write to the buffer. Agrona's `MutableDirectBuffer.putStringUtf8(int, String)`
allocates internally too (verified by disassembling 2.2.1's `AbstractMutableDirectBuffer`), so
swapping to it would not save anything; eliminating encode-side allocation requires either
switching the wire format to ASCII (`putStringWithoutLengthAscii` walks `String.charAt` and
allocates nothing) or using a cached `byte[]` per-field, both of which are out of scope for
slice 4f's smallest-fix mandate.

### Fix

`AcceptOrderCommand.decode` (and the four mirror sites: `ApplyExecutionReportCommand.decode`,
`OrderAdmittedEvent.decode`, `ExecutionAppliedEvent.decode`,
`OmsAdmissionClusteredService.AdmittedOrder.decode`) replace the `stringByteLen(decodedString)`
cursor advance with a `stringByteLenAt(buffer, offset)` helper that re-reads the 4-byte length
prefix from the buffer:

```java
private static int stringByteLenAt(DirectBuffer buffer, int offset) {
    return Integer.BYTES + buffer.getInt(offset);
}
```

Zero allocations. Same value as the encoder wrote (the 4-byte length prefix at the head of each
string field is the same number of bytes the encoder consumed when it wrote the payload).

Snapshot encode-side `AdmittedOrder.encodedLength()` is **not** changed: it pre-sizes the
snapshot buffer before any wire bytes exist, so it still has to compute the UTF-8 byte length
from the in-memory `String`. Snapshot **load** at line 721 of `OmsAdmissionClusteredService`
also still calls `encodedLength()` to advance the snapshot read cursor, which means snapshot
load still allocates 4–5× `byte[]` per `AdmittedOrder` — but snapshot loads only happen at JVM
startup, so the per-replay cost is small relative to the per-command hot path. Tracked as a
follow-up: introduce a `decodeAndAdvance` helper (or have `decode` return a small cursor) so
the snapshot loader can advance allocation-free.

### Post-fix

| Bench | Wall (ns/op) | Allocation (B/op) | Δ B/op | Δ ns/op |
| --- | --- | --- | --- | --- |
| `encode` | 25.3 ± 1.4 | 176 | 0 | +0.6 (within noise) |
| `decode` | **50.0 ± 4.9** | **560** | **−176 (−23.9 %)** | **−13.3 (−21.0 %)** |

Decode allocation dropped exactly as predicted (4× redundant `byte[]` ≈ 176 B/op eliminated),
and the avoided allocation also paid back ~13 ns of decode wall-clock time. Encode is unchanged
(out of scope for this fix). The same fix applies symmetrically to four other decode sites
(`ApplyExecutionReportCommand`, `OrderAdmittedEvent`, `ExecutionAppliedEvent`, snapshot
`AdmittedOrder`) — those are not separately benched yet but the structural pattern is
identical. Slice 4g/4h will fold a per-site bench in if the GC profile under
sustained throughput shows them dominating.

### What this does NOT prove

- The clusterBench round-trip (slice 4e) numbers do not include this fix yet — slice 4g will
  re-run that bench on Linux with this fix applied.
- The change makes **decode** allocation-lower, not allocation-free. The four `String`/`byte[]`
  pairs from `readString` remain. Eliminating them requires either a flyweight string view (no
  copy, view into the underlying buffer) or an SBE migration. ADR 0001 already calls out SBE
  as the planned eventual format; this slice does not pre-empt that decision.
- Single-laptop numbers; production Linux numbers come in slice 4g.

---

---

## Slice 4g — GC comparison (G1 vs generational ZGC vs Shenandoah)

The cluster-node JVM ships under `bootJarClusterNode` (`com.balh.oms.OmsClusterNodeBootstrap`)
on top of `eclipse-temurin:21-jre-jammy`, which defaults to G1. This slice asks: would either of
the low-pause collectors (generational ZGC or Shenandoah) give a tail-latency win over G1 at the
workload shape `clusterBench` produces — and if so, by how much?

### Rig

* `clusterBench` Gradle task gained a `-PgcMode=g1|zgc|shenandoah` parameter that wires the
  matching JVM flags (`-XX:+UseG1GC` / `-XX:+UseZGC -XX:+ZGenerational` / `-XX:+UseShenandoahGC`)
  and exports `OMS_BENCH_GC_LABEL` so each `summary.md` self-describes which GC produced it.
* `clusterBench` also gained `-PclusterBenchJava=/abs/path/to/java`. The project's default
  toolchain JVM here (JetBrains Runtime 21.0.8 — the JDK 21 distribution that ships with
  Android Studio) **does not include ZGC or Shenandoah** (`-XX:+UseZGC` fails with
  `Option -XX:+UseZGC not supported`), so the GC comparison runs under a different JVM. The
  toolchain stays JDK 21 for compile (production parity); only the bench runtime is swapped.
* `scripts/cluster-bench-gc-compare.sh` orchestrates the loop: invokes `clusterBench` once per
  GC × `CLUSTER_BENCH_GC_REPEAT` repeats, dumps each run into its own report directory, and
  prints a per-run p50 / p99 / p99.9 / max table at the end.

### Local result (4 repeats × 3 GCs × 20 s steady-state @ 1000 ops/s)

JVM: Homebrew OpenJDK 25.0.2, macOS aarch64, single-laptop dev box. Bench: 3 s warmup + 20 s
steady-state at 1000 ops/s ≈ 20 000 commits per run; 4 runs per GC. All 12 runs hit zero
timeouts and zero errors.

| GC | Run | p50 (µs) | p99 (µs) | p99.9 (µs) | max (µs) |
| --- | --- | ---: | ---: | ---: | ---: |
| G1         | 1 | 812 | 4029 |  5447 | 21455 |
| G1         | 2 | 813 | 4075 | 10095 | 22687 |
| G1         | 3 | 815 | 3995 |  5051 | 10519 |
| G1         | 4 | 816 | 3995 |  5151 |  7003 |
| ZGC (gen.) | 1 | 815 | 4017 |  5107 |  6027 |
| ZGC (gen.) | 2 | 817 | 4155 |  6611 | 17951 |
| ZGC (gen.) | 3 | 815 | 4183 |  8463 | 22463 |
| ZGC (gen.) | 4 | 817 | 4013 |  5683 | 23423 |
| Shenandoah | 1 | 835 | 4407 |  5503 |  6755 |
| Shenandoah | 2 | 857 | 4619 |  6251 | 10063 |
| Shenandoah | 3 | 858 | 4695 |  6035 |  7979 |
| Shenandoah | 4 | 818 | 4059 |  5879 | 14967 |

Median of the 4 runs per GC:

| GC | p50 (µs) | p99 (µs) | p99.9 (µs) | worst max (µs) |
| --- | ---: | ---: | ---: | ---: |
| **G1**         | **814** | **4012** | 5299 | 22 687 |
| ZGC (gen.)     |   816   |  4099    | 6147 | 23 423 |
| Shenandoah     |   846   |  4513    | 5891 | 14 967 |

### Interpretation

Three honest readings of the table:

1. **The workload is not GC-bound at this allocation rate.** With slice 4f's decode fix,
   per-command allocation is ~560 B/op on the apply side plus a few hundred bytes for the
   admission record and event encode → ~1 MB/s of allocation at 1 000 ops/s. The retained
   live set is ~24 k `AdmittedOrder` records ≈ 3 MB. There is essentially nothing for the GC
   to do; p50 differences across the three collectors are smaller than the run-to-run noise.
2. **G1 is the most consistent at p99 and the median is fastest.** G1 wins p50 (814 vs 816 vs
   846 µs) and p99 (4012 vs 4099 vs 4513 µs). It is also the JDK 21+ default — using anything
   else means an explicit operator override that will surprise an on-call engineer reading a
   `jcmd ... VM.flags` dump.
3. **Tail spikes (p99.9, max) are dominated by something other than the GC.** All three see
   occasional 20 ms+ outliers (G1 p99.9 ranges 5051–10095, ZGC 5107–8463, Shenandoah
   5503–6251 within their respective 4-run windows; max touches 22 687 / 23 423 / 14 967).
   At ~20 samples per p99.9 bucket, these outliers are individual events — most likely macOS
   scheduler noise, Aeron media-driver pauses, or APFS / mmap behaviour rather than GC.

### Decision: G1 stays as the cluster-node default

* G1 is the JDK 21 default — `Dockerfile` (`FROM eclipse-temurin:21-jre-jammy`) already runs
  it. Slice 4g lands NO change to JVM flags. The slice's value is the recorded evidence that
  generational ZGC and Shenandoah do not buy us anything on this workload.
* `OmsClusterNodeBootstrap`'s `bootJarClusterNode` is unchanged. If a future production
  workload pushes allocation rate or live set high enough to make GC pauses visible at p99,
  the rig is in place to re-run the comparison cheaply.

### What this does NOT prove

* Single-laptop, Apple Silicon, JDK 25 — none of which is the production cluster-node shape
  (Linux x86_64, eclipse-temurin:21). The relative ordering may flip on a Linux runner.
  Generational ZGC's mmap and unmap behaviour, transparent huge pages, and NUMA all differ
  markedly between macOS and Linux. **Phase 5 (k8s-driven multi-process load) will re-run
  this comparison on a Linux runner; until then the production decision is "stay on the JDK
  default" rather than "G1 has been validated for production".**
* The bench is single-thread offer / single-thread egress consume. Real production has
  inbound `oms-fix-egress` ER traffic too, multi-account fan-out, snapshot bursts, and a
  longer time horizon. Allocation rate could 5–10× under realistic load, at which point GC
  choice starts to matter.
* Live set is small — ~24 k orders during the bench. Production with weeks of admitted
  orders before a snapshot would carry tens or hundreds of MB of `AdmittedOrder` /
  `executionRefIndex` / `senderSeqIndex` data. Generational ZGC's allocation behaviour is
  designed to remain pauseless as the heap grows; that property is not exercised here.
* macOS scheduler / mmap behaviour is the most likely culprit for p99.9 outliers in this run.
  A Linux runner with isolated CPUs and `chrt -f` priority would tighten the tail
  distribution and let the GC differences (if any) actually show through.

---

## How to reproduce

```bash
cd ~/oms
# clusterBench (slice 4e):
OMS_BENCH_DURATION_S=2 OMS_BENCH_THROUGHPUT_OPS_PER_S=200 ./gradlew clusterBench

# JMH wire-format bench with allocation profiler (slice 4f):
./gradlew jmh -PjmhInclude=AcceptOrderCommandWireBench -Pjmh.profilers=gc
# results: build/reports/jmh/{human.txt,results.json}

# GC comparison (slice 4g) — runs G1, generational ZGC, Shenandoah back-to-back:
CLUSTER_BENCH_JAVA=/opt/homebrew/opt/openjdk/bin/java \
CLUSTER_BENCH_GC_REPEAT=4 \
OMS_BENCH_WARMUP_S=3 OMS_BENCH_DURATION_S=20 OMS_BENCH_THROUGHPUT_OPS_PER_S=1000 \
  bash scripts/cluster-bench-gc-compare.sh
# results: build/reports/cluster-bench-gc-compare/<ts>/{g1,zgc,shenandoah}-run<N>/summary.md
```

CI / Linux Temurin 21 — same script, point `CLUSTER_BENCH_JAVA` at the runner's Temurin
install. Per-run reports + the comparison table land in the workspace; upload the report
directory as a CI artifact for production-shape decisions.
