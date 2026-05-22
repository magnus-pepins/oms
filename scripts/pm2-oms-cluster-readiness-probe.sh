#!/usr/bin/env bash
# PM2 / cron readiness probe for OMS cluster admission (Phase 5).
#
# Polls ingress GET /actuator/oms-cluster-readiness until HTTP 200 and status READY.
# Readiness is published on oms-ingress (Aeron counter reader), not on cluster-node HTTP.
#
# Usage:
#   bash scripts/pm2-oms-cluster-readiness-probe.sh
#   OMS_READINESS_PROBE_URL=http://127.0.0.1:8088/actuator/oms-cluster-readiness \
#     MAX_ATTEMPTS=48 POLL_INTERVAL_SEC=5 bash scripts/pm2-oms-cluster-readiness-probe.sh
#
# Exit 0 when READY; exit 1 on timeout or non-READY. Does not mutate cluster state.
#
# ecosystem.config.cjs documents optional wait_ready on oms-cluster-node — enable on pop
# only after Phase 2–5 ingress jars are deployed (plans/oms-cluster-recovery-and-hardening.md §5.3).

set -euo pipefail

OMS_READINESS_PROBE_URL="${OMS_READINESS_PROBE_URL:-http://127.0.0.1:8088/actuator/oms-cluster-readiness}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-24}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-5}"

CURL_OPTS=(--silent --show-error --max-time 10)

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  body=""
  status=""
  if body=$(curl "${CURL_OPTS[@]}" -w '\n%{http_code}' "$OMS_READINESS_PROBE_URL" 2>/dev/null); then
    status=$(printf '%s' "$body" | tail -n1)
    json=$(printf '%s' "$body" | sed '$d')
    if [[ "$status" == "200" ]] && echo "$json" | grep -q '"status":"READY"'; then
      echo "READY (attempt ${attempt}/${MAX_ATTEMPTS})"
      exit 0
    fi
    echo "attempt ${attempt}/${MAX_ATTEMPTS}: http=${status} body=$(echo "$json" | head -c 200)"
  else
    echo "attempt ${attempt}/${MAX_ATTEMPTS}: curl failed"
  fi
  if [[ "$attempt" -lt "$MAX_ATTEMPTS" ]]; then
    sleep "$POLL_INTERVAL_SEC"
  fi
done

echo "NOT READY after ${MAX_ATTEMPTS} attempts (${OMS_READINESS_PROBE_URL})" >&2
exit 1
