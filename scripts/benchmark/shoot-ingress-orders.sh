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
#   OMS_INTERNAL_API_KEY (required)
#   OMS_URL              default http://127.0.0.1:8088
#   SHOOT_COUNT          default 50
#   SHOOT_SLEEP_MS       default 0   (sleep between POSTs)
#   SHOOT_INSTRUMENT     default AAPL
#   SHOOT_QTY            default 1
#   SHOOT_LIMIT          default 150

set -euo pipefail

random_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())"
}

OMS_URL="${OMS_URL:-http://127.0.0.1:8088}"
KEY="${OMS_INTERNAL_API_KEY:?set OMS_INTERNAL_API_KEY}"
COUNT="${SHOOT_COUNT:-50}"
SLEEP_MS="${SHOOT_SLEEP_MS:-0}"
SYM="${SHOOT_INSTRUMENT:-AAPL}"
QTY="${SHOOT_QTY:-1}"
LIM="${SHOOT_LIMIT:-150}"

ok=0
fail=0
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
  code="$(curl -sS -o /tmp/oms-shoot-last.json -w '%{http_code}' \
    -X POST "$OMS_URL/internal/v1/orders" \
    -H "Content-Type: application/json" \
    -H "X-OMS-Internal-Key: $KEY" \
    -d "$BODY")"
  if [[ "$code" == "201" || "$code" == "200" ]]; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
    echo "FAIL i=$i http=$code" >&2
    head -c 500 /tmp/oms-shoot-last.json >&2 || true
    echo >&2
  fi
  if [[ "$SLEEP_MS" != "0" ]]; then
    python3 -c "import time; time.sleep(${SLEEP_MS}/1000.0)"
  fi
done

echo "done: ok=$ok fail=$fail (expected 201 for new orders; 200 if you reused idempotency keys accidentally)"
