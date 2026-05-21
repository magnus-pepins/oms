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

# V41 cross-currency cash legs land on @FX-Suspense-<ccy> first (cash-base in
# cashCurrency, cash-quote in tradeCurrency) before the bank's nostro picks
# up the tradeCurrency side. The pair sits at zero in steady state and is
# allowed to overdraft (see LedgerSettlementLegPoster#legBody) because the
# matched leg may post first and rebalance later. Seeding the balances here
# is required so the poster does not fail with BALANCE_NOT_FOUND on first use.
FX_SUSPENSE=(
  "USD|@FX-Suspense-USD|FX cross-currency settlement suspense USD (demo)"
  "EUR|@FX-Suspense-EUR|FX cross-currency settlement suspense EUR (demo)"
  "GBP|@FX-Suspense-GBP|FX cross-currency settlement suspense GBP (demo)"
)

# Stock commission platform revenue (Bug H: @Platform-Revenue-<CCY>, not bare @Platform-Revenue).
PLATFORM_REVENUE=(
  "USD|@Platform-Revenue-USD|Platform stock commission revenue USD (demo)"
  "EUR|@Platform-Revenue-EUR|Platform stock commission revenue EUR (demo)"
)

curl_ledger() {
  curl -sS -H "Authorization: Bearer ${LEDGER_KEY}" -H 'Content-Type: application/json' "$@"
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

seed_group() {
  local label="$1"; shift
  local specs=("$@")
  echo "--- ${label} ---"
  for spec in "${specs[@]}"; do
    IFS='|' read -r ccy ind desc <<< "${spec}"
    existing="$(find_existing "${ind}")"
    if [[ -z "${existing}" ]]; then
      echo "Creating ${ind} (${ccy})..."
      bid="$(create_balance "${ccy}" "${ind}" "${desc}")"
      echo "  created ${ind}=${bid}"
    else
      bid="${existing}"
      echo "${ind} already exists as ${bid}"
    fi
  done
}

seed_group "Fees revenue (V40 single-currency fee leg)" "${FEES[@]}"
seed_group "FX suspense (V41 cross-currency cash legs)" "${FX_SUSPENSE[@]}"
seed_group "Platform revenue (stock commission fee leg)" "${PLATFORM_REVENUE[@]}"

echo
echo "=========================================================="
echo "Seed complete. Fee leg targets: @Fees-{USD,EUR,GBP}"
echo "Cross-currency cash leg suspense: @FX-Suspense-{USD,EUR,GBP}"
echo "Platform revenue (stock): @Platform-Revenue-{USD,EUR}"
echo "=========================================================="
