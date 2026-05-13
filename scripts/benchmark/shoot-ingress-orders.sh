#!/usr/bin/env bash
# Fire many POST /internal/v1/orders against a running multi-JVM OMS topology
# (cluster-node + ingress-replica + projector + optional fix-egress).
#
# REWRITTEN in slice 4i for the post-Phase-3 multi-JVM topology. The previous monolith
# version is gone — see oms/docs/runbooks/local-multi-jvm-bench.md for bring-up.
#
# What you get:
# - HTTP `time_total` p50/p95/p99 from the curl loop (ingress-replica HTTP commit window).
# - Per-role Prometheus deltas: ingress-replica timers (`oms.pipeline.ingress.accept` slice 1,
#   `oms.cluster.client.commit_round_trip` slice 4c) + projector / fix-egress lag gauges
#   (slice 4d) + cluster-node snapshot freshness (slice 4h).
# - A derived end-of-run "ingress→NOS upper bound" = commit_round_trip_p99 + fix_egress_lag_seconds.
#   NOT a per-order histogram (the slice-3b-2 cross-JVM cut deleted that single-process sample).
#   For HdrHistogram-grade cluster-path tail latency use `./gradlew clusterBench` (slice 4e).
#
# Prereqs (Pop! server / Linux dev box; macOS works for HTTP-only side):
#   curl, jq, python3
#
# Required env:
#   OMS_INTERNAL_API_KEY   must match the running ingress-replica's oms.http.internal-api-key.
#
# Optional env (defaults are localhost):
#   OMS_URL                          http://127.0.0.1:8088              ingress-replica HTTP
#   OMS_INGRESS_REPLICA_PROM_URL     http://127.0.0.1:8087/actuator/prometheus
#   OMS_POSTGRES_PROJECTOR_PROM_URL  http://127.0.0.1:8090/actuator/prometheus
#   OMS_FIX_EGRESS_PROM_URL          http://127.0.0.1:8091/actuator/prometheus
#   OMS_CLUSTER_NODE_METRICS_URL     http://127.0.0.1:8089/metrics
#
# Tuning:
#   SHOOT_COUNT          50     number of orders to send
#   SHOOT_SLEEP_MS       0      sleep between POSTs (ms)
#   SHOOT_INSTRUMENT     AAPL
#   SHOOT_QTY            1
#   SHOOT_LIMIT          150
#   SHOOT_PRINT_EACH     0      set 1 to print per-request HTTP time on stderr

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DELTA_PROM_PY="$SCRIPT_DIR/summarize_cluster_pipeline_deltas.py"

random_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())"
}

# --- env defaults (config-and-limits: no bare numerics in the body) ---
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

COUNT="${SHOOT_COUNT:-50}"
SLEEP_MS="${SHOOT_SLEEP_MS:-0}"
SYM="${SHOOT_INSTRUMENT:-AAPL}"
QTY="${SHOOT_QTY:-1}"
LIM="${SHOOT_LIMIT:-150}"

TIMES_FILE="$(mktemp)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH" "$TIMES_FILE"' EXIT

scrape() {
  local url="$1"
  local out="$2"
  if [[ -z "$url" ]]; then
    return 0
  fi
  curl -fsS "$url" -o "$out" 2>/dev/null || true
}

# --- baseline scrapes (ingress-replica is the only one we need pre AND post for
# histogram deltas; projector / fix-egress / cluster-node we read at the end so the
# gauge values reflect end-of-run state, which is what the SLO doc describes) ---
scrape "$INGRESS_PROM_URL" "$SCRATCH/ingress_before.txt"

ok=0
created=0
fail=0
hinted_401=false

for i in $(seq 1 "$COUNT"); do
  CLIENT_KEY="bench-shoot-$(date +%s)-$i-$RANDOM"
  ACCOUNT_ID="$(random_uuid)"
  BODY="$(jq -nc \
    --arg cid "$CLIENT_KEY" \
    --arg aid "$ACCOUNT_ID" \
    --arg sym "$SYM" \
    --arg qty "$QTY" \
    --arg lim "$LIM" \
    '{accountId:$aid,clientIdempotencyKey:$cid,side:"BUY",instrumentSymbol:$sym,quantity:$qty,limitPrice:$lim,timeInForce:"DAY"}')"
  meta="$(curl -sS -o /tmp/oms-shoot-last.json -w '%{http_code}|%{time_total}' \
    -X POST "$OMS_URL/internal/v1/orders" \
    -H "Content-Type: application/json" \
    -H "X-OMS-Internal-Key: $KEY" \
    -d "$BODY")"
  IFS='|' read -r code time_s <<< "$meta"
  if [[ "$code" == "201" || "$code" == "200" ]]; then
    ok=$((ok + 1))
    if [[ "$code" == "201" ]]; then
      created=$((created + 1))
    fi
    printf '%s\n' "$time_s" >>"$TIMES_FILE"
    if [[ "${SHOOT_PRINT_EACH:-0}" == "1" ]]; then
      echo "i=$i http=$code time_total_s=$time_s" >&2
    fi
  else
    fail=$((fail + 1))
    echo "FAIL i=$i http=$code" >&2
    if [[ "$code" == "401" && "$hinted_401" == "false" ]]; then
      hinted_401=true
      echo "  401: header X-OMS-Internal-Key must exactly match OMS_INTERNAL_API_KEY on the running ingress-replica." >&2
      echo "  Restart with the same export you use here, e.g. export OMS_INTERNAL_API_KEY='your-secret'" >&2
    fi
    head -c 500 /tmp/oms-shoot-last.json >&2 || true
    echo >&2
  fi
  if [[ "$SLEEP_MS" != "0" ]]; then
    python3 -c "import time; time.sleep(${SLEEP_MS}/1000.0)"
  fi
done

echo "done: ok=$ok (http 201+200)  created=$created (http 201)  fail=$fail"

# --- end-of-run scrapes ---
scrape "$INGRESS_PROM_URL"        "$SCRATCH/ingress_after.txt"
scrape "$PROJECTOR_PROM_URL"      "$SCRATCH/projector_after.txt"
scrape "$FIX_EGRESS_PROM_URL"     "$SCRATCH/fix_egress_after.txt"
scrape "$CLUSTER_NODE_METRICS_URL" "$SCRATCH/cluster_node_after.txt"

echo ""
python3 "$DELTA_PROM_PY" \
  --ingress-before "$SCRATCH/ingress_before.txt" \
  --ingress-after "$SCRATCH/ingress_after.txt" \
  --projector-after "$SCRATCH/projector_after.txt" \
  --fix-egress-after "$SCRATCH/fix_egress_after.txt" \
  --cluster-node-after "$SCRATCH/cluster_node_after.txt" \
  --created "$created" \
  || true

# --- HTTP only (curl); seconds ---
if [[ "$ok" -gt 0 ]]; then
  echo ""
  echo "HTTP only — curl time_total to ingress-replica (seconds; client-observed, includes JSON marshalling):"
  python3 - "$TIMES_FILE" <<'PY'
import sys
from pathlib import Path

p = Path(sys.argv[1])
xs = [float(line) for line in p.read_text().splitlines() if line.strip()]
if not xs:
    print("  (no samples)")
    raise SystemExit(0)
xs.sort()
n = len(xs)

def pct(q: float) -> float:
    if n == 1:
        return xs[0]
    idx = (n - 1) * q
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    w = idx - lo
    return xs[lo] * (1 - w) + xs[hi] * w

mean = sum(xs) / n
print(f"  n={n}  min={xs[0]:.6f}  max={xs[-1]:.6f}  mean={mean:.6f}")
print(f"  p50={pct(0.50):.6f}  p95={pct(0.95):.6f}  p99={pct(0.99):.6f}")
PY
else
  echo ""
  echo "No successful HTTP responses; skipping curl latency summary."
fi
