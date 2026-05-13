#!/usr/bin/env bash
# Phase 4 slice 4k wrapper: drive `IngressBurstMain` (concurrent JDK 21 HttpClient burst)
# bracketed by Prometheus pre/post scrapes against a running multi-JVM OMS topology
# (cluster-node + ingress-replica + projector + optional fix-egress).
#
# This is the high-concurrency counterpart to the slice-4i `shoot-ingress-orders.sh`. The
# shoot script's serial curl loop is bounded at ~1 request per ~21 ms by curl/jq spawn
# overhead, which makes it impossible to tell whether the FIX-egress's per-NOS cost is the
# saturation point or the curl loop's inter-request gap. This script fires a configurable
# concurrent burst (default 50 in-flight) so the slice-4j histograms read the egress's
# actual per-event latency under load.
#
# How:
# 1. pre-scrape: ingress-replica, postgres-projector, fix-egress
# 2. ./gradlew bootRunBurst (JDK 21 HttpClient + virtual threads, HdrHistogram of HTTP RTT)
# 3. post-scrape: same three + cluster-node
# 4. summarize_cluster_pipeline_deltas.py: per-event histograms (slice 4j)
#
# Required env:
#   OMS_INTERNAL_API_KEY   must match the running ingress-replica's oms.http.internal-api-key.
#
# Optional env (defaults; mirrors shoot-ingress-orders.sh + IngressBurstMain):
#   OMS_URL                          http://127.0.0.1:8088              ingress-replica HTTP
#   OMS_BURST_URLS                   (unset)                            slice 4m: comma-separated
#                                    full target URLs (e.g.
#                                    http://127.0.0.1:8088/internal/v1/orders,http://127.0.0.1:8095/internal/v1/orders)
#                                    when set, IngressBurstMain round-robins requests across the
#                                    listed targets — used to drive N ingress-replicas without an
#                                    external load balancer. Overrides OMS_URL / OMS_BURST_URL.
#   OMS_INGRESS_REPLICA_PROM_URL     http://127.0.0.1:8087/actuator/prometheus
#   OMS_POSTGRES_PROJECTOR_PROM_URL  http://127.0.0.1:8090/actuator/prometheus
#   OMS_FIX_EGRESS_PROM_URL          http://127.0.0.1:8091/actuator/prometheus
#   OMS_CLUSTER_NODE_METRICS_URL     http://127.0.0.1:8089/metrics
#
# Burst tuning (forwarded to IngressBurstMain — config-and-limits compliant):
#   OMS_BURST_TOTAL          1000   total requests
#   OMS_BURST_CONCURRENCY    50     max in-flight requests
#   OMS_BURST_RPS_CAP        0      optional RPS cap (0 = unlimited)
#   OMS_BURST_ACCOUNT_POOL   1      number of distinct accountId UUIDs to reuse
#   OMS_BURST_INSTRUMENT     AAPL
#   OMS_BURST_QUANTITY       1
#   OMS_BURST_LIMIT_PRICE    150
#   OMS_BURST_REQUEST_TIMEOUT_S  30
#   OMS_BURST_WARMUP         0      requests whose RTT is excluded from the HdrHistogram

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DELTA_PROM_PY="$SCRIPT_DIR/summarize_cluster_pipeline_deltas.py"
OMS_REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

OMS_URL="${OMS_URL:-http://127.0.0.1:8088}"
INGRESS_PROM_URL="${OMS_INGRESS_REPLICA_PROM_URL:-http://127.0.0.1:8087/actuator/prometheus}"
PROJECTOR_PROM_URL="${OMS_POSTGRES_PROJECTOR_PROM_URL:-http://127.0.0.1:8090/actuator/prometheus}"
FIX_EGRESS_PROM_URL="${OMS_FIX_EGRESS_PROM_URL:-http://127.0.0.1:8091/actuator/prometheus}"
CLUSTER_NODE_METRICS_URL="${OMS_CLUSTER_NODE_METRICS_URL:-http://127.0.0.1:8089/metrics}"

KEY="${OMS_INTERNAL_API_KEY:?set OMS_INTERNAL_API_KEY (must match the running ingress-replica)}"
if [[ "$KEY" == *"…"* || "$KEY" == "replace-me-internal-key" ]]; then
  echo "Refusing to run: OMS_INTERNAL_API_KEY looks like a documentation placeholder." >&2
  echo "Set a real secret and use the identical value when starting the ingress-replica" >&2
  echo "(oms.http.internal-api-key)." >&2
  exit 1
fi

SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

scrape() {
  local url="$1"
  local out="$2"
  if [[ -z "$url" ]]; then
    return 0
  fi
  curl -fsS "$url" -o "$out" 2>/dev/null || true
}

# --- baseline scrapes ---
scrape "$INGRESS_PROM_URL"    "$SCRATCH/ingress_before.txt"
scrape "$PROJECTOR_PROM_URL"  "$SCRATCH/projector_before.txt"
scrape "$FIX_EGRESS_PROM_URL" "$SCRATCH/fix_egress_before.txt"

# --- run the burst (Gradle bootRunBurst -> IngressBurstMain) ---
# Gradle args are the burst tool's env vars; we forward OMS_URL into OMS_BURST_URL by suffix.
BURST_URL="${OMS_BURST_URL:-${OMS_URL%/}/internal/v1/orders}"
# Slice 4m: when OMS_BURST_URLS is set, forward it untouched and the burst tool round-robins
# across the list. OMS_BURST_URL stays valid as a single-target fallback.
BURST_URLS="${OMS_BURST_URLS:-}"

if [[ -n "$BURST_URLS" ]]; then
  echo "Burst targets (OMS_BURST_URLS, round-robin):"
  echo "  $BURST_URLS" | tr ',' '\n' | sed 's/^[[:space:]]*/  /'
else
  echo "Burst target (single OMS_BURST_URL):"
  echo "  $BURST_URL"
fi

(
  cd "$OMS_REPO_DIR"
  OMS_BURST_URL="$BURST_URL" \
  OMS_BURST_URLS="$BURST_URLS" \
  OMS_INTERNAL_API_KEY="$KEY" \
    ./gradlew --console=plain --no-daemon -q bootRunBurst
)

# --- end-of-run scrapes ---
scrape "$INGRESS_PROM_URL"        "$SCRATCH/ingress_after.txt"
scrape "$PROJECTOR_PROM_URL"      "$SCRATCH/projector_after.txt"
scrape "$FIX_EGRESS_PROM_URL"     "$SCRATCH/fix_egress_after.txt"
scrape "$CLUSTER_NODE_METRICS_URL" "$SCRATCH/cluster_node_after.txt"

echo ""
# --created not provided: the burst tool already prints a status breakdown including 201 count.
python3 "$DELTA_PROM_PY" \
  --ingress-before "$SCRATCH/ingress_before.txt" \
  --ingress-after "$SCRATCH/ingress_after.txt" \
  --projector-before "$SCRATCH/projector_before.txt" \
  --projector-after "$SCRATCH/projector_after.txt" \
  --fix-egress-before "$SCRATCH/fix_egress_before.txt" \
  --fix-egress-after "$SCRATCH/fix_egress_after.txt" \
  --cluster-node-after "$SCRATCH/cluster_node_after.txt" \
  || true
