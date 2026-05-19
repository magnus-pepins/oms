#!/usr/bin/env bash
#
# Seed Bank fee-revenue balances in Ledger for the OMS settlement-outbox demo.
#
# Each settled trade enqueues two Ledger legs (V39 outbox `leg_kind`):
#   * cash   — inv-<accountId>-<ccy> → @Nostro-<ccy>-Bank   (seeded by seed-fx-nostros.sh)
#   * fee    — inv-<accountId>-<ccy> → @Fees-<ccy>          (seeded by this script)
#
# Fees revenue accumulates here without ever being issued — it stays positive
# as customers pay commissions and is treated as P&L until swept to retained
# earnings (not modelled in the demo).
#
# Idempotent: creating an existing indicator is skipped; no funds are issued.

set -euo pipefail

LEDGER_BASE="${LEDGER_BASE:-http://127.0.0.1:30100}"
LEDGER_KEY="${LEDGER_KEY:-gf12umbgh}"
LEDGER_ID="${LEDGER_ID:-customer_ledger_id}"

FEES=(
  "USD|@Fees-USD|Bank fee revenue USD (demo)"
  "EUR|@Fees-EUR|Bank fee revenue EUR (demo)"
  "GBP|@Fees-GBP|Bank fee revenue GBP (demo)"
)

curl_ledger() {
  curl -sS -H "X-Ledger-Key: ${LEDGER_KEY}" -H 'Content-Type: application/json' "$@"
}

find_existing() {
  local indicator="$1"
  curl_ledger "${LEDGER_BASE}/balances?indicator=${indicator}" \
    | python3 -c "import sys,json; rows=json.load(sys.stdin); print(rows[0]['balanceId'] if rows else '', end='')"
}

create_balance() {
  local ccy="$1" indicator="$2" desc="$3"
  curl_ledger -X POST "${LEDGER_BASE}/balances" \
    -d "{\"ledgerId\":\"${LEDGER_ID}\",\"currency\":\"${ccy}\",\"indicator\":\"${indicator}\",\"metaData\":{\"kind\":\"fees\",\"label\":\"${desc}\",\"managed_by\":\"oms-settlement-demo\"}}" \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print(r['balanceId'], end='')"
}

CSV=""
for spec in "${FEES[@]}"; do
  IFS='|' read -r ccy ind desc <<< "${spec}"
  existing="$(find_existing "${ind}")"
  if [[ -z "${existing}" ]]; then
    echo "Creating ${ind} (${ccy})..."
    bid="$(create_balance "${ccy}" "${ind}" "${desc}")"
    echo "  created fees=${bid}"
  else
    bid="${existing}"
    echo "${ind} already exists as ${bid}"
  fi
  if [[ -n "${CSV}" ]]; then CSV="${CSV},"; fi
  CSV="${CSV}${ccy}=${ind}"
done

echo
echo "=========================================================="
echo "OMS_SETTLEMENT_FEES_INDICATORS_CSV='${CSV}'"
echo "=========================================================="
