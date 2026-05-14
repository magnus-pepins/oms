#!/usr/bin/env bash
# Phase 4 slice 4p: launch a single OMS multi-JVM-bench role in the background.
#
# Sources ~/.oms-bench.env (so child JVMs inherit the bench env), then nohup's
# ./gradlew bootRun<Role> with stdout+stderr to ~/oms/logs/<role>.log. Designed
# for the local-multi-jvm-bench runbook on Pop! / single-Linux-host benches; not
# for production. Each role is a separate Spring Boot context (cluster-node /
# postgres-projector / fix-egress / ingress-replica), each backed by its own
# media-driver-side-channel into the shared Aeron Cluster directory.
#
# Usage:
#   ./scripts/launch-bench-stack.sh cluster-node
#   ./scripts/launch-bench-stack.sh projector
#   ./scripts/launch-bench-stack.sh fix-egress
#   ./scripts/launch-bench-stack.sh ingress-1     # OMS_HTTP_PORT=8088 OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8087
#   ./scripts/launch-bench-stack.sh ingress-2     # OMS_HTTP_PORT=8188 OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8187
#
# After: tail -f ~/oms/logs/<role>.log; pgrep -af 'com.balh.oms\.[A-Z]' to list JVMs.

set -u

ROLE=${1:-all}
LOG=$HOME/oms/logs
mkdir -p "$LOG"
cd "$HOME/oms"

# shellcheck disable=SC1091
. "$HOME/.oms-bench.env"

launch() {
  local task=$1
  local logfile=$2
  echo "[launch] $task -> $LOG/$logfile"
  nohup ./gradlew --no-daemon -q "$task" >"$LOG/$logfile" 2>&1 &
  echo $!
}

case "$ROLE" in
  cluster-node)
    launch bootRunClusterNode cluster-node.log
    ;;
  projector)
    launch bootRunPostgresProjector projector.log
    ;;
  fix-egress)
    launch bootRunFixEgress fix-egress.log
    ;;
  ingress-1)
    OMS_HTTP_PORT=8088 OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8087 \
      launch bootRunIngressReplica ingress-1.log
    ;;
  ingress-2)
    OMS_HTTP_PORT=8188 OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT=8187 \
      launch bootRunIngressReplica ingress-2.log
    ;;
  *)
    echo "unknown role $ROLE; valid: cluster-node|projector|fix-egress|ingress-1|ingress-2" >&2
    exit 2
    ;;
esac
