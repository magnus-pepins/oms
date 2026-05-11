#!/usr/bin/env bash
# Smoke: POST one internal order, then print OpenTelemetry Prometheus lines for ingress→FIX NOS latency.
# Requires: OMS running with oms.routing.backend=fix, FIX session logged on, OTel metrics enabled, and a broker/acceptor.
#
# Usage:
#   export OMS_INTERNAL_API_KEY=...
#   export OMS_URL=http://127.0.0.1:8088
#   export OTEL_PROMETHEUS_URL=http://127.0.0.1:9464/metrics   # default when OMS_OTEL_METRICS_ENABLED=true
#   ./scripts/benchmark/ingress-to-fix-nos-smoke.sh
#
# After a successful NOS, scrape should show non-zero _count / _bucket for oms_fix_ingress_to_nos (exact suffix
# may vary slightly by exporter version; use grep below).

set -euo pipefail

OMS_URL="${OMS_URL:-http://127.0.0.1:8088}"
OTEL_PROMETHEUS_URL="${OTEL_PROMETHEUS_URL:-http://127.0.0.1:9464/metrics}"
KEY="${OMS_INTERNAL_API_KEY:?set OMS_INTERNAL_API_KEY}"

CLIENT_KEY="bench-smoke-$(date +%s)-$RANDOM"
ACCOUNT_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"

BODY="$(jq -nc \
  --arg cid "$CLIENT_KEY" \
  --arg aid "$ACCOUNT_ID" \
  '{accountId:$aid,clientIdempotencyKey:$cid,side:"BUY",instrumentSymbol:"AAPL",quantity:"1",limitPrice:"150",timeInForce:"DAY"}')"

echo "POST $OMS_URL/internal/v1/orders"
START_MS="$(python3 -c 'import time; print(int(time.time()*1000))')"
HTTP_CODE="$(curl -sS -o /tmp/oms-smoke-order.json -w '%{http_code}' \
  -X POST "$OMS_URL/internal/v1/orders" \
  -H "Content-Type: application/json" \
  -H "X-OMS-Internal-Key: $KEY" \
  -d "$BODY")"
END_MS="$(python3 -c 'import time; print(int(time.time()*1000))')"
ELAPSED_MS=$((END_MS - START_MS))

echo "HTTP status: $HTTP_CODE  (wall client time ~ ${ELAPSED_MS}ms — includes network + OMS + response)"
if [[ "$HTTP_CODE" != "201" && "$HTTP_CODE" != "200" ]]; then
  cat /tmp/oms-smoke-order.json >&2 || true
  exit 1
fi

echo ""
echo "OTel Prometheus scrape: $OTEL_PROMETHEUS_URL (grep ingress_to_nos)"
if ! curl -fsS "$OTEL_PROMETHEUS_URL" | grep -E 'ingress_to_nos|ingress_to_nos_samples_discarded' || true; then
  echo "(no matching lines — enable OMS_OTEL_METRICS_ENABLED=true and wait until NOS is sent; histogram appears after first successful send)"
fi
