#!/usr/bin/env bash
# Place a synthetic order against a running OMS Ingress for demo / smoke testing.
#
# Subcommands map to the deterministic scenarios baked into the FIX loopback acceptor
# (src/test/java/com/balh/oms/fix/it/FixRoundTripAcceptorApplication.java):
#
#   market <SYM> [QTY]              -> MARKET order, instant FILL
#   limit  <SYM> [QTY] [PX]         -> LIMIT order, rests WORKING until cancel/replace
#   partial <SYM> [QTY] [PX]        -> Symbol-trigger PARTIAL: partial fill then full fill
#   newrej <SYM> [QTY] [PX]         -> Symbol-trigger NEWREJ: place a LIMIT against the
#                                      'NEWREJ' symbol; broker sends ER ET=8 REJECTED so the
#                                      order never books (status → REJECTED).
#   cxlrej <SYM> [QTY] [PX]         -> Symbol-trigger CXLREJ (was 'reject-prep'/'REJECT'):
#                                      place a LIMIT against the 'CXLREJ' symbol; subsequent
#                                      cancel/replace will fail with 35=9 OrderCancelReject.
#
# Required env (typically from ~/.oms-bench.env):
#   OMS_INTERNAL_API_KEY   - shared secret for /internal/v1/orders
#
# Optional env:
#   OMS_INGRESS_BASE_URL   - defaults to http://127.0.0.1:8088
#
# Examples:
#   scripts/demo-place-order.sh market AAPL 1
#   scripts/demo-place-order.sh limit  AAPL 1 150.00
#   scripts/demo-place-order.sh partial PARTIAL 4 100.00
#   scripts/demo-place-order.sh newrej NEWREJ 1 50.00
#   scripts/demo-place-order.sh cxlrej CXLREJ 1 50.00

set -euo pipefail

BASE_URL="${OMS_INGRESS_BASE_URL:-http://127.0.0.1:8088}"
if [[ -z "${OMS_INTERNAL_API_KEY:-}" ]]; then
  echo "OMS_INTERNAL_API_KEY not set. Source ~/.oms-bench.env or export it." >&2
  exit 2
fi

usage() {
  sed -n '2,30p' "$0"
  exit "${1:-2}"
}

ACTION="${1:-}"
SYM="${2:-}"
QTY="${3:-1}"
PX="${4:-}"

if [[ -z "$ACTION" || -z "$SYM" ]]; then
  usage
fi

ACC="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen)"
KEY="demo-${ACTION}-$(date +%s)-$$"

case "$ACTION" in
  market)
    PRICE_FIELD=""
    ;;
  limit|partial|newrej|cxlrej|reject-prep)
    : "${PX:=150.00}"
    PRICE_FIELD=",\"limitPrice\":\"${PX}\""
    ;;
  *)
    usage
    ;;
esac

BODY=$(cat <<JSON
{
  "accountId": "${ACC}",
  "clientIdempotencyKey": "${KEY}",
  "side": "BUY",
  "instrumentSymbol": "${SYM}",
  "quantity": "${QTY}",
  "timeInForce": "DAY"${PRICE_FIELD}
}
JSON
)

echo "=== ${ACTION} order: SYM=${SYM} QTY=${QTY}${PX:+ PX=${PX}} ACC=${ACC} ==="
RESP=$(curl -sS -o /tmp/oms-resp.json -w "HTTP %{http_code}" \
  -X POST "${BASE_URL}/internal/v1/orders" \
  -H "Content-Type: application/json" \
  -H "X-OMS-Internal-Key: ${OMS_INTERNAL_API_KEY}" \
  -d "${BODY}")
echo "${RESP}"

ID=$(jq -r .id /tmp/oms-resp.json 2>/dev/null || true)
echo "Order id: ${ID}"

# Brief settle so the FIX leg and projector have time to react before we read back.
sleep 1

echo "=== STATUS ==="
curl -fsS -H "X-OMS-Internal-Key: ${OMS_INTERNAL_API_KEY}" \
  "${BASE_URL}/internal/v1/desk/orders/snapshot?limit=10" \
  | jq --arg id "${ID}" '.orders[] | select(.id == $id) | {id, symbol: .instrumentSymbol, status, version}'
