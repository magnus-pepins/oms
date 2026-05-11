#!/usr/bin/env bash
# Minimal drill: verify OMS health after a simulated "standby" would attach to PG.
# Usage: OMS_BASE_URL=http://localhost:8088 ./scripts/drills/drill-failover.sh
set -euo pipefail
BASE="${OMS_BASE_URL:-http://localhost:8088}"
code=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/actuator/health")
if [[ "${code}" != "200" ]]; then
  echo "FAIL: expected HTTP 200 from ${BASE}/actuator/health, got ${code}"
  exit 1
fi
echo "OK: health check ${BASE}/actuator/health -> ${code}"
