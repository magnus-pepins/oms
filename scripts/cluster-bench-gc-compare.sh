#!/usr/bin/env bash
# Phase 4 slice 4g — run `clusterBench` once per GC and compare tail latency.
#
# Reads the bench config from env (same env vars OmsClusterBenchHarness already honours), then
# loops over G1, ZGC, Shenandoah, calling the `clusterBench` Gradle task with `-PgcMode=...`. Each
# run gets its own report directory so summaries are not overwritten. Prints a one-line per-GC
# summary at the end (p50 / p99 / p99.9 µs from each summary.md) for quick comparison.
#
# Why a shell driver and not a Gradle compound task? A shell script is the smallest possible
# orchestrator; one source of truth for what runs in what order; trivially copy-pasteable into a
# CI workflow on a Linux runner where the production decision actually lives.
#
# JVM selection
#   The default toolchain JVM (e.g. JetBrains Runtime in this repo) does not always ship ZGC or
#   Shenandoah. Override the bench JVM by setting CLUSTER_BENCH_JAVA to a `java` binary that does
#   (e.g. Temurin 21 from `/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home/bin/java`
#   or Homebrew `openjdk@21`). Production decisions must come from a JDK + OS shape that matches
#   the cluster-node Docker base (Temurin 21 on Linux, eclipse-temurin:21-jre-jammy as of slice 4g).
#
# Caveats / not-claims
#   * Single-laptop numbers are illustrative for picking a sensible default, not a substitute for
#     a Linux production runner. ZGC and Shenandoah's mmap and unmap behaviour, transparent huge
#     pages, and NUMA all differ markedly between macOS and Linux.
#   * Each GC runs once. To detect run-to-run variance, set CLUSTER_BENCH_GC_REPEAT > 1 (the
#     script then runs each GC that many times and prints all runs).

set -euo pipefail
cd "$(dirname "$0")/.."

: "${OMS_BENCH_WARMUP_S:=2}"
: "${OMS_BENCH_DURATION_S:=10}"
: "${OMS_BENCH_THROUGHPUT_OPS_PER_S:=500}"
: "${OMS_BENCH_TIMEOUT_MS:=5000}"
: "${CLUSTER_BENCH_GC_LIST:=g1 zgc shenandoah}"
: "${CLUSTER_BENCH_GC_REPEAT:=1}"

export OMS_BENCH_WARMUP_S OMS_BENCH_DURATION_S OMS_BENCH_THROUGHPUT_OPS_PER_S OMS_BENCH_TIMEOUT_MS

REPORT_ROOT="build/reports/cluster-bench-gc-compare/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$REPORT_ROOT"

JAVA_OVERRIDE_ARG=""
if [[ -n "${CLUSTER_BENCH_JAVA:-}" ]]; then
  JAVA_OVERRIDE_ARG="-PclusterBenchJava=${CLUSTER_BENCH_JAVA}"
  echo "Using JVM: ${CLUSTER_BENCH_JAVA}"
  "${CLUSTER_BENCH_JAVA}" -version 2>&1 | sed 's/^/  /'
else
  echo "Using JVM: Gradle toolchain default ($(./gradlew -q help --task wrapper >/dev/null 2>&1 ; echo see ./gradlew --version))"
  ./gradlew --version 2>&1 | grep -E "JVM:" | sed 's/^/  /'
fi
echo "Report root: $REPORT_ROOT"
echo "Bench config: warmup=${OMS_BENCH_WARMUP_S}s, duration=${OMS_BENCH_DURATION_S}s, throughput=${OMS_BENCH_THROUGHPUT_OPS_PER_S} ops/s"
echo

for gc in $CLUSTER_BENCH_GC_LIST; do
  for run in $(seq 1 "$CLUSTER_BENCH_GC_REPEAT"); do
    run_dir="$REPORT_ROOT/${gc}-run${run}"
    mkdir -p "$run_dir"
    echo "==> Running ${gc} (run ${run}/${CLUSTER_BENCH_GC_REPEAT})"
    OMS_BENCH_REPORT_DIR="$run_dir" \
    OMS_BENCH_AERON_DIR_BASE="$run_dir/aeron" \
      ./gradlew --quiet clusterBench -PgcMode="$gc" $JAVA_OVERRIDE_ARG
  done
done

echo
echo "==================================================================="
echo "Comparison (p50 / p99 / p99.9 / max µs across runs):"
echo "==================================================================="
printf "%-15s  %-8s  %10s  %10s  %12s  %10s\n" "gc" "run" "p50" "p99" "p99.9" "max"
for gc in $CLUSTER_BENCH_GC_LIST; do
  for run in $(seq 1 "$CLUSTER_BENCH_GC_REPEAT"); do
    summary="$REPORT_ROOT/${gc}-run${run}/summary.md"
    if [[ ! -f "$summary" ]]; then
      printf "%-15s  %-8s  %s\n" "$gc" "$run" "MISSING summary.md"
      continue
    fi
    p50=$(awk -F'|' '/^\| p50 \|/    {gsub(/ /,"",$3); print $3}' "$summary")
    p99=$(awk -F'|' '/^\| p99 \|/    {gsub(/ /,"",$3); print $3}' "$summary")
    p999=$(awk -F'|' '/^\| p99\.9 \|/{gsub(/ /,"",$3); print $3}' "$summary")
    max=$(awk -F'|' '/^\| max \|/    {gsub(/ /,"",$3); print $3}' "$summary")
    printf "%-15s  %-8s  %10s  %10s  %12s  %10s\n" "$gc" "$run" "$p50" "$p99" "$p999" "$max"
  done
done
echo
echo "Reports: $REPORT_ROOT"
