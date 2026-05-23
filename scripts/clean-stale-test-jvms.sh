#!/usr/bin/env bash
# Kill stale Gradle test worker JVMs that survived a previous test run and
# are still holding the JVM-wide Aeron Cluster test ports (20510-20550, see
# `AbstractPostgresIntegrationTest$TestAeronClusterSingleton`).
#
# Symptom this addresses:
#   org.agrona.concurrent.AgentTerminationException: ...
#   io.aeron.exceptions.AeronException: ERROR - channel error - Address already
#     in use (localhost:20550)
#   ...thrown when ClusteredMediaDriver tries to bind a port already owned by
#   a GradleWorkerMain JVM from a previous run that was killed externally
#   (SIGKILL bypasses our JVM shutdown hook in TestAeronClusterSingleton).
#
# This script is idempotent and safe to run any time no `./gradlew :test`
# invocation is in progress. It will NOT touch deployed OMS processes — it
# only matches the local JVM signatures Gradle uses for forked test workers.
#
# Usage:
#   ./scripts/clean-stale-test-jvms.sh           # report + kill -9
#   ./scripts/clean-stale-test-jvms.sh --dry-run # report only
#
# Implementation notes: Apple ships bash 3.2, which lacks `mapfile`. This
# script is intentionally portable to bash 3.x — no associative arrays, no
# `mapfile`, no `readarray`. Run it via `bash` directly or as an executable
# script; do NOT `source` it.
set -euo pipefail

DRY_RUN="no"
if [ "${1:-}" = "--dry-run" ] || [ "${1:-}" = "-n" ]; then
  DRY_RUN="yes"
fi

# Aeron test cluster ports (see TestAeronClusterSingleton).
AERON_TEST_PORTS="20510 20520 20530 20540 20550"

WORKER_PIDS=$(pgrep -f 'worker\.org\.gradle\.process\.internal\.worker\.GradleWorkerMain' 2>/dev/null || true)

# Also find any process holding one of the Aeron test ports — captures stale
# media drivers that may have outlived a worker even when pgrep returns empty.
PORT_PIDS=""
for port in $AERON_TEST_PORTS; do
  # `lsof -ti` returns just PIDs (numeric, one per line).
  found=$(lsof -ti UDP:"$port" 2>/dev/null || true)
  if [ -n "$found" ]; then
    PORT_PIDS="$PORT_PIDS
$found"
  fi
done

UNIQ=$(printf "%s\n%s\n" "$WORKER_PIDS" "$PORT_PIDS" | awk 'NF' | sort -u)

if [ -z "$UNIQ" ]; then
  echo "clean-stale-test-jvms: no stale GradleWorkerMain JVMs found, no test ports held."
  exit 0
fi

echo "clean-stale-test-jvms: candidates:"
echo "$UNIQ" | while IFS= read -r pid; do
  ps -o pid=,user=,etime=,command= -p "$pid" 2>/dev/null | sed 's/^/  /' || true
done

if [ "$DRY_RUN" = "yes" ]; then
  echo "clean-stale-test-jvms: --dry-run set, not killing."
  exit 0
fi

echo "clean-stale-test-jvms: sending SIGKILL…"
echo "$UNIQ" | while IFS= read -r pid; do
  kill -9 "$pid" 2>/dev/null || true
done

sleep 1

STILL_BOUND=""
for port in $AERON_TEST_PORTS; do
  if lsof -i UDP:"$port" >/dev/null 2>&1; then
    STILL_BOUND="$STILL_BOUND $port"
  fi
done

if [ -n "$STILL_BOUND" ]; then
  echo "clean-stale-test-jvms: WARNING - ports still bound after SIGKILL:$STILL_BOUND"
  echo "  inspect with: lsof -i UDP:$(echo $STILL_BOUND | awk '{print $1}')"
  exit 1
fi

echo "clean-stale-test-jvms: done; all Aeron test ports free."
