#!/usr/bin/env bash
# Fire many POST /internal/v1/orders against a running OMS (synthetic load for latency / Prometheus).
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
#   OMS_URL              default http://127.0.0.1:8088
#   SHOOT_COUNT          default 50
#   SHOOT_SLEEP_MS       default 0   (sleep between POSTs)
#   SHOOT_INSTRUMENT     default AAPL
#   SHOOT_QTY            default 1
#   SHOOT_LIMIT          default 150
#   SHOOT_PRINT_EACH     default 0   (set to 1 to print each request's HTTP time on stderr)
#
# Latency note:
#   curl %{time_total} is client wall time until the HTTP response completes (TLS/TCP + server work).
#   That is NOT the same as OTel histogram oms.fix.ingress_to_nos (accept → FIX NOS on wire), which
#   can lag after 201. For that, scrape OMS_OTEL_PROMETHEUS_PORT (default 9464) and use histogram_quantile.

set -euo pipefail

random_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())"
}

OMS_URL="${OMS_URL:-http://127.0.0.1:8088}"
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

hinted_401=false
ok=0
fail=0
TIMES_FILE="$(mktemp)"
trap 'rm -f "$TIMES_FILE"' EXIT

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

echo "done: ok=$ok fail=$fail (expected 201 for new orders; 200 if you reused idempotency keys accidentally)"

if [[ "$ok" -gt 0 ]]; then
  echo ""
  echo "HTTP POST time_total (seconds, curl client wall — see script header for OTel vs HTTP):"
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
  echo "No successful responses; skipping HTTP latency summary."
fi
