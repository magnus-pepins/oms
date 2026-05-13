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

## How to reproduce

```bash
cd ~/oms
# clusterBench (slice 4e):
OMS_BENCH_DURATION_S=2 OMS_BENCH_THROUGHPUT_OPS_PER_S=200 ./gradlew clusterBench

# JMH wire-format bench with allocation profiler (slice 4f):
./gradlew jmh -PjmhInclude=AcceptOrderCommandWireBench -Pjmh.profilers=gc
# results: build/reports/jmh/{human.txt,results.json}
```
