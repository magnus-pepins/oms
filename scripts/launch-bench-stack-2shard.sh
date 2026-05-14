#!/usr/bin/env bash
# Phase 4 Tier 2.5 phase E-3a: launch a single 2-shard OMS multi-JVM-bench role on Pop!.
#
# E-3a is the cheap-experiment-first path: bring up TWO independent OMS Aeron Cluster
# vertical stacks side-by-side on one host (each with its own aeronDirBase, port set, and
# Postgres database) and bench them in parallel to falsify the multi-cluster-lift
# hypothesis BEFORE the full E-3b Spring multi-bean refactor.
#
# Both shards run at oms.shard.count=1 (router invariant unchanged) so no Spring code
# changes ship with this slice — the only Java change is making
# OmsClusterNodeBootstrap.buildArchiveContext()'s controlChannel env-overridable so two
# cluster-node JVMs can co-exist (otherwise both bind localhost:8010 and the second
# fails). FIX-egress is intentionally NOT launched for either shard to remove the FIX
# port-collision variable and isolate the cluster-substrate scaling question; ledger
# inflight is also disabled (env layered below) so a shared Ledger balance does not
# confound aggregate rps. Compare the single-shard rps measured here against the
# two-shard aggregate to isolate the cluster substrate's parallelism on Pop!.
#
# Topology (per shard):
#   - cluster-node     OmsClusterNodeBootstrap
#   - postgres-projector (via Spring profile oms-postgres-projector)
#   - ingress-replica  (via Spring profile oms-ingress-replica)
#
# Port plan:
#   shard 0 (defaults):
#     cluster-members  0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010
#     ingress http     8088   ingress mgmt 8087   cluster metrics 8089
#     projector http   8093   projector mgmt 8090
#     aeronDirBase     $HOME/oms/build/aeron-cluster
#     OMS_PG_URL       jdbc:postgresql://127.0.0.1:6543/oms
#
#   shard 1 (offset by +1000 / +100 on management/HTTP ports):
#     cluster-members  0,localhost:21110,localhost:21220,localhost:21330,localhost:21440,localhost:9010
#     archive ctrl     aeron:udp?endpoint=localhost:9010   (NEW env: OMS_AERON_ARCHIVE_CONTROL_CHANNEL)
#     ingress http     8188   ingress mgmt 8187   cluster metrics 9089
#     projector http   9093   projector mgmt 9090
#     aeronDirBase     $HOME/oms/build/aeron-cluster-1
#     OMS_PG_URL       jdbc:postgresql://127.0.0.1:6543/oms_1
#
# Sources ~/.oms-bench.env for the credentials/api-key surface, then layers shard-specific
# env on top. Shard-specific env can be tuned via ~/.oms-bench-2shard-shard{0,1}.env if
# the operator wants to override per-shard knobs (otherwise the script's inline defaults
# below are used).
#
# Usage:
#   ./scripts/launch-bench-stack-2shard.sh cluster-node-0
#   ./scripts/launch-bench-stack-2shard.sh cluster-node-1
#   ./scripts/launch-bench-stack-2shard.sh projector-0
#   ./scripts/launch-bench-stack-2shard.sh projector-1
#   ./scripts/launch-bench-stack-2shard.sh ingress-0
#   ./scripts/launch-bench-stack-2shard.sh ingress-1
#
# After: tail -f ~/oms/logs/<role>.log; pgrep -af 'com.balh.oms\.[A-Z]' to list JVMs.
#
# Pre-flight on first use only (one-time, idempotent):
#   docker exec ledger-supabase-db psql -U postgres -c 'CREATE DATABASE oms_1;'
#
# This script is for the local-multi-jvm-bench runbook on Pop! single-Linux-host benches;
# it is not a production launch path.

set -u

ROLE=${1:-}
LOG=$HOME/oms/logs
mkdir -p "$LOG"
cd "$HOME/oms"

# shellcheck disable=SC1091
. "$HOME/.oms-bench.env"

# E-3a: disable FIX-egress (port collision avoidance — see header) and ledger inflight
# reservation (single shared balance would otherwise cap aggregate rps at the Ledger
# Balance OCC ceiling, masking cluster-substrate parallelism). Operators who want the
# full topology (FIX + ledger) for E-3a can override these in
# ~/.oms-bench-2shard-shard{0,1}.env after wiring per-shard FIX SenderCompID + ports.
export OMS_FIX_AUTO_START=false
export OMS_ROUTING_BACKEND=noop
export OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED=false
export OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=false

# Per-shard env: shard 0 keeps existing single-host defaults; shard 1 lives on a
# port-offset namespace and a separate Postgres DB. shardId on each side is 0 (router
# invariant); shardCount=1 (no multi-bean injection yet).
shard_env_0() {
  export OMS_AERON_DIR_BASE="$HOME/oms/build/aeron-cluster"
  export OMS_AERON_CLUSTER_MEMBERS="0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010"
  # Default OMS_AERON_ARCHIVE_CONTROL_CHANNEL ("aeron:udp?endpoint=localhost:8010") is
  # intentionally left unset on shard 0 so the JVM picks the bootstrap's
  # DEFAULT_ARCHIVE_CONTROL_CHANNEL — keeps shard 0 byte-identical with pre-E-3a runs.
  export OMS_CLUSTER_NODE_METRICS_PORT=8089
  export OMS_POSTGRES_PROJECTOR_AERON_DIR="$OMS_AERON_DIR_BASE/media-driver"
  export OMS_POSTGRES_PROJECTOR_HTTP_PORT=8093
  export OMS_POSTGRES_PROJECTOR_MANAGEMENT_SERVER_PORT=8090
  export OMS_HTTP_PORT=8088
  export OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8087
  export OMS_CLUSTER_CLIENT_AERON_DIRECTORY="$OMS_AERON_DIR_BASE/media-driver"
  export OMS_CLUSTER_CLIENT_INGRESS_ENDPOINTS="0=localhost:20110"
  export OMS_PG_URL='jdbc:postgresql://127.0.0.1:6543/oms?prepareThreshold=0&socketTimeout=10'
  if [[ -f "$HOME/.oms-bench-2shard-shard0.env" ]]; then
    # shellcheck disable=SC1091
    . "$HOME/.oms-bench-2shard-shard0.env"
  fi
}

shard_env_1() {
  export OMS_AERON_DIR_BASE="$HOME/oms/build/aeron-cluster-1"
  export OMS_AERON_CLUSTER_MEMBERS="0,localhost:21110,localhost:21220,localhost:21330,localhost:21440,localhost:9010"
  export OMS_AERON_ARCHIVE_CONTROL_CHANNEL="aeron:udp?endpoint=localhost:9010"
  export OMS_CLUSTER_NODE_METRICS_PORT=9089
  export OMS_POSTGRES_PROJECTOR_AERON_DIR="$OMS_AERON_DIR_BASE/media-driver"
  export OMS_POSTGRES_PROJECTOR_HTTP_PORT=9093
  export OMS_POSTGRES_PROJECTOR_MANAGEMENT_SERVER_PORT=9090
  export OMS_HTTP_PORT=8188
  export OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8187
  export OMS_CLUSTER_CLIENT_AERON_DIRECTORY="$OMS_AERON_DIR_BASE/media-driver"
  export OMS_CLUSTER_CLIENT_INGRESS_ENDPOINTS="0=localhost:21110"
  export OMS_PG_URL='jdbc:postgresql://127.0.0.1:6543/oms_1?prepareThreshold=0&socketTimeout=10'
  if [[ -f "$HOME/.oms-bench-2shard-shard1.env" ]]; then
    # shellcheck disable=SC1091
    . "$HOME/.oms-bench-2shard-shard1.env"
  fi
}

launch() {
  local task=$1
  local logfile=$2
  echo "[launch] $task -> $LOG/$logfile"
  nohup ./gradlew --no-daemon -q "$task" >"$LOG/$logfile" 2>&1 &
  echo $!
}

case "$ROLE" in
  cluster-node-0)
    shard_env_0
    launch bootRunClusterNode cluster-node-0.log
    ;;
  cluster-node-1)
    shard_env_1
    launch bootRunClusterNode cluster-node-1.log
    ;;
  projector-0)
    shard_env_0
    launch bootRunPostgresProjector projector-0.log
    ;;
  projector-1)
    shard_env_1
    launch bootRunPostgresProjector projector-1.log
    ;;
  ingress-0)
    shard_env_0
    launch bootRunIngressReplica ingress-0.log
    ;;
  ingress-1)
    shard_env_1
    launch bootRunIngressReplica ingress-1.log
    ;;
  *)
    echo "unknown role '$ROLE'; valid: cluster-node-{0,1}|projector-{0,1}|ingress-{0,1}" >&2
    exit 2
    ;;
esac
