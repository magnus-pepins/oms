#!/usr/bin/env bash
# Fire many POST /internal/v1/orders against a running OMS (synthetic load + latency).
#
# Prereqs: curl, jq, python3; OMS up with internal API key; unique idempotency key per iteration.
#
# Typical local stack (3 terminals):
#   1) ./gradlew fixLoopbackAcceptor
#   2) OMS bootRun with OMS_ROUTING_BACKEND=fix OMS_FIX_AUTO_START=true (and matching FIX comp ids / port)
#   3) ./scripts/benchmark/shoot-ingress-orders.sh
#
# Env:
#   OMS_INTERNAL_API_KEY (required — must match running OMS, same as OMS_INTERNAL_API_KEY on bootRun)
#   OMS_URL              default http://127.0.0.1:8088  (used for default actuator URL)
#   SHOOT_COUNT          default 50
#   SHOOT_SLEEP_MS       default 0   (sleep between POSTs)
#   SHOOT_INSTRUMENT     default AAPL
#   SHOOT_QTY            default 1
#   SHOOT_LIMIT          default 150
#   SHOOT_PRINT_EACH     default 0   (set to 1 to print each request's HTTP time on stderr)
#
# Full-flow (THIS BATCH only):
#   SHOOT_OTEL_METRICS_URL — e.g. http://127.0.0.1:9464/metrics with OMS_OTEL_METRICS_ENABLED=true.
#   The script saves OTel scrape before POSTs and after the wait; printed mean/pXX are **deltas**, not lifetime.
#
#   SHOOT_PROMETHEUS_URL — default ${OMS_URL}/actuator/prometheus for Micrometer oms.pipeline.* breakdown
#   (where time went: ingress vs outbox→Chronicle vs control.apply vs FIX).
#
#   SHOOT_OTEL_WAIT_MAX_SEC      default 120
#   SHOOT_OTEL_POLL_INTERVAL_SEC default 1
#   SHOOT_OTEL_SHOW_RAW          default 0 — dump raw OTel histogram lines

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARSE_PY="$SCRIPT_DIR/parse_otel_ingress_to_nos_histogram.py"
DELTA_PROM_PY="$SCRIPT_DIR/summarize_micrometer_pipeline_deltas.py"

random_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())"
}

OMS_URL="${OMS_URL:-http://127.0.0.1:8088}"
PROM_URL="${SHOOT_PROMETHEUS_URL:-${OMS_URL%/}/actuator/prometheus}"
KEY="${OMS_INTERNAL_API_KEY:?set OMS_INTERNAL_API_KEY (must match the running OMS process — same value as bootRun / OMS_INTERNAL_API_KEY)}"
if [[ "$KEY" == *"…"* || "$KEY" == "replace-me-internal-key" ]]; then
  echo "Refusing to run: OMS_INTERNAL_API_KEY looks like a documentation placeholder." >&2
  echo "Set a real secret and use the identical value when starting OMS (oms.http.internal-api-key)." >&2
  exit 1
fi
COUNT="${SHOOT_COUNT:-50}"
SLEEP_MS="${SHOOT_SLEEP_MS:-0}"
SYM="${SHOOT_INSTRUMENT:-AAPL}"
QTY="${SHOOT_QTY:-1}"
LIM="${SHOOT_LIMIT:-150}"
OTEL_WAIT_MAX_SEC="${SHOOT_OTEL_WAIT_MAX_SEC:-120}"
OTEL_POLL_SEC="${SHOOT_OTEL_POLL_INTERVAL_SEC:-1}"

TIMES_FILE="$(mktemp)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH" "$TIMES_FILE"' EXIT

curl -fsS "$PROM_URL" -o "$SCRATCH/prom_before.txt" 2>/dev/null || true

hinted_401=false
ok=0
created=0
fail=0

BASELINE_OTEL="NA"
if [[ -n "${SHOOT_OTEL_METRICS_URL:-}" ]]; then
  if curl -fsS "${SHOOT_OTEL_METRICS_URL}" -o "$SCRATCH/otel_before.txt" 2>/dev/null; then
    BASELINE_OTEL="$(python3 "$PARSE_PY" count <"$SCRATCH/otel_before.txt")"
  else
    rm -f "$SCRATCH/otel_before.txt"
  fi
fi

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
      echo "  401: header X-OMS-Internal-Key must exactly match OMS_INTERNAL_API_KEY on the running OMS." >&2
      echo "  If OMS was started without OMS_INTERNAL_API_KEY, oms.http.internal-api-key is empty and every request is rejected." >&2
      echo "  Restart OMS with the same export you use here, e.g. export OMS_INTERNAL_API_KEY='your-secret'" >&2
    fi
    head -c 500 /tmp/oms-shoot-last.json >&2 || true
    echo >&2
  fi
  if [[ "$SLEEP_MS" != "0" ]]; then
    python3 -c "import time; time.sleep(${SLEEP_MS}/1000.0)"
  fi
done

echo "done: ok=$ok (http 201+200)  created=$created (http 201 only; OTel full-flow counts these)  fail=$fail"

# --- FULL FLOW OTel: THIS BATCH (delta before vs after scrape), ms ---
if [[ -n "${SHOOT_OTEL_METRICS_URL:-}" ]]; then
  echo ""
  echo "======================================================================"
  echo "FULL FLOW (this batch) — DB commit (201) → FIX NOS  |  OTel, ms, Δ scrapes"
  echo "======================================================================"
  if [[ "$created" -eq 0 ]]; then
    echo "  No HTTP 201 in this run — OTel histogram only starts on new orders."
  elif [[ ! -f "$SCRATCH/otel_before.txt" ]]; then
    echo "  No baseline OTel scrape (curl failed at script start)."
  else
    if [[ "$BASELINE_OTEL" == "NA" ]]; then
      echo "  Note: baseline scrape had no ingress_to_nos histogram yet (cold OTel / first scrape) — using baseline count 0."
      BASELINE_OTEL=0
    fi
    target=$((BASELINE_OTEL + created))
    poll_start=$SECONDS
    poll_deadline=$((SECONDS + OTEL_WAIT_MAX_SEC))
    last_metrics=""
    curl_rc=1
    last_cur="NA"
    while (( SECONDS < poll_deadline )); do
      set +e
      last_metrics="$(curl -fsS "${SHOOT_OTEL_METRICS_URL}" 2>&1)"
      curl_rc=$?
      set -e
      if [[ "$curl_rc" -eq 0 && -n "$last_metrics" ]]; then
        last_cur="$(echo "$last_metrics" | python3 "$PARSE_PY" count)"
        if [[ "$last_cur" != "NA" ]] && [[ "$last_cur" -ge "$target" ]]; then
          printf '%s\n' "$last_metrics" >"$SCRATCH/otel_after.txt"
          break
        fi
      fi
      sleep "$OTEL_POLL_SEC"
    done
    waited=$((SECONDS - poll_start))
    if [[ ! -f "$SCRATCH/otel_after.txt" ]]; then
      printf '%s\n' "${last_metrics:-}" >"$SCRATCH/otel_after.txt" || true
    fi
    if [[ "$curl_rc" -ne 0 ]]; then
      echo "  scrape failed (curl exit $curl_rc) — last message:" >&2
      while IFS= read -r line || [[ -n "$line" ]]; do echo "    $line" >&2; done <<< "${last_metrics:-}"
      echo "  Fix: OMS_OTEL_METRICS_ENABLED=true and SHOOT_OTEL_METRICS_URL (default :9464/metrics)." >&2
    elif [[ ! -s "$SCRATCH/otel_after.txt" ]]; then
      echo "  (empty OTel scrape after run)" >&2
    else
      if [[ "${last_cur:-NA}" == "NA" ]]; then
        last_cur=0
      fi
      if [[ "$last_cur" -lt "$target" ]]; then
        echo "  TIMEOUT after ${waited}s: global _count is ${last_cur}, need >= ${target} (baseline ${BASELINE_OTEL} + ${created})."
        echo "  Using last scrape for delta anyway (partial batch):"
      else
        echo "  Histogram global count reached ${target} (waited ${waited}s)."
      fi
      echo ""
      python3 "$PARSE_PY" delta-summary "$SCRATCH/otel_before.txt" "$SCRATCH/otel_after.txt" | while IFS= read -r line; do echo "  $line"; done
      echo ""
      echo "  count_this_run = new observations between first and last scrape (should match created=${created})."
      if [[ "${SHOOT_OTEL_SHOW_RAW:-0}" == "1" ]]; then
        echo ""
        echo "  --- raw OTel lines (ingress_to_nos) from last scrape ---"
        grep -E 'ingress_to_nos' "$SCRATCH/otel_after.txt" || true
      fi
    fi
  fi
else
  echo ""
  echo "======================================================================"
  echo "FULL FLOW OTel — skipped (set SHOOT_OTEL_METRICS_URL + OMS_OTEL_METRICS_ENABLED=true)"
  echo "======================================================================"
  echo "  Example: export SHOOT_OTEL_METRICS_URL='http://127.0.0.1:9464/metrics'"
fi

# --- Micrometer pipeline (THIS BATCH): where time went ---
curl -fsS "$PROM_URL" -o "$SCRATCH/prom_after.txt" 2>/dev/null || true
python3 "$DELTA_PROM_PY" "$SCRATCH/prom_before.txt" "$SCRATCH/prom_after.txt" || true

# --- HTTP only (curl); seconds ---
if [[ "$ok" -gt 0 ]]; then
  echo ""
  echo "HTTP only — curl time_total until response ends (seconds; NOT full flow to FIX):"
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
