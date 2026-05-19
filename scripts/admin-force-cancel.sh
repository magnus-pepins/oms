#!/usr/bin/env bash
# Force-cancel one or more OMS orders without a broker round-trip, via the internal admin
# endpoint POST /internal/v1/admin/orders/{orderId}/force-cancel.
#
# Usage:
#   scripts/admin-force-cancel.sh <orderId> [reason]
#   scripts/admin-force-cancel.sh --symbol <SYMBOL> [--status WORKING] [reason]
#
# In the second form we read /internal/v1/desk/orders/snapshot, filter rows whose
# instrumentSymbol matches --symbol (and optionally --status), and issue a force-cancel for
# each one. Useful for cleaning up clusters of stuck orders left by simulator-trigger demos
# (e.g. the WORKING orders against the legacy 'REJECT' / new 'CXLREJ' symbol).
#
# Required env (typically from ~/.oms-bench.env):
#   OMS_INTERNAL_API_KEY   - shared secret for /internal/v1/**
#
# Optional env:
#   OMS_INGRESS_BASE_URL   - defaults to http://127.0.0.1:8088
#
# Examples:
#   scripts/admin-force-cancel.sh 0976b76d-1819-4f12-9ac4-8bfcc92dc2b9 "cleanup demo leftovers"
#   scripts/admin-force-cancel.sh --symbol REJECT --status WORKING "Wed-demo cleanup"
#   scripts/admin-force-cancel.sh --symbol CXLREJ "post-rename cleanup"

set -euo pipefail

BASE_URL="${OMS_INGRESS_BASE_URL:-http://127.0.0.1:8088}"
if [[ -z "${OMS_INTERNAL_API_KEY:-}" ]]; then
  echo "OMS_INTERNAL_API_KEY not set. Source ~/.oms-bench.env or export it." >&2
  exit 2
fi

usage() {
  sed -n '2,25p' "$0"
  exit "${1:-2}"
}

post_cancel() {
  local orderId="$1"
  local reason="${2:-cli admin force-cancel}"
  local body
  body=$(printf '{"reason":%s}' "$(printf '%s' "$reason" | jq -Rs .)")
  printf 'force-cancel %s ... ' "$orderId"
  local resp
  resp=$(curl -sS -o /tmp/oms-admin-cancel.json -w "HTTP %{http_code}" \
    -X POST "${BASE_URL}/internal/v1/admin/orders/${orderId}/force-cancel" \
    -H "Content-Type: application/json" \
    -H "X-OMS-Internal-Key: ${OMS_INTERNAL_API_KEY}" \
    -d "$body")
  printf '%s ' "$resp"
  jq -c . /tmp/oms-admin-cancel.json
}

if [[ "${1:-}" == "--symbol" ]]; then
  shift
  SYMBOL="${1:-}"
  shift || true
  STATUS=""
  if [[ "${1:-}" == "--status" ]]; then
    shift
    STATUS="${1:-}"
    shift || true
  fi
  REASON="${1:-cli admin force-cancel by symbol=${SYMBOL}}"

  if [[ -z "$SYMBOL" ]]; then
    usage
  fi

  # Pull the snapshot (bounded by OMS_DESK_SNAPSHOT_MAX_LIMIT, default 500) and pick out
  # matching orders. Filter client-side; the snapshot endpoint deliberately doesn't push
  # status / symbol filters to SQL (see DeskSnapshotController javadoc).
  TMP=$(mktemp)
  curl -fsS -H "X-OMS-Internal-Key: ${OMS_INTERNAL_API_KEY}" \
    "${BASE_URL}/internal/v1/desk/orders/snapshot?limit=500" \
    > "$TMP"

  JQ_FILTER='.orders[] | select(.instrumentSymbol == $sym)'
  if [[ -n "$STATUS" ]]; then
    JQ_FILTER='.orders[] | select(.instrumentSymbol == $sym and .status == $st)'
  fi
  IDS=$(jq -r --arg sym "$SYMBOL" --arg st "$STATUS" "$JQ_FILTER | .id" "$TMP")
  rm -f "$TMP"

  if [[ -z "$IDS" ]]; then
    echo "no matching orders found for symbol=${SYMBOL}${STATUS:+ status=${STATUS}}"
    exit 0
  fi
  COUNT=$(echo "$IDS" | wc -l | tr -d ' ')
  echo "matched ${COUNT} order(s); force-cancelling..."
  while IFS= read -r OID; do
    post_cancel "$OID" "$REASON"
  done <<< "$IDS"
  echo
  echo "done. Allow ~1s for projector to flip status to CANCELLED, then re-read snapshot."
  exit 0
fi

ORDER_ID="${1:-}"
REASON="${2:-cli admin force-cancel}"
if [[ -z "$ORDER_ID" ]]; then
  usage
fi
post_cancel "$ORDER_ID" "$REASON"
