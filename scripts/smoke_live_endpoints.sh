#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
REFRESH_TIMEOUT="${REFRESH_TIMEOUT:-600}"
GET_TIMEOUT="${GET_TIMEOUT:-120}"

echo "==> Smoke pass against ${BASE_URL}"

check_json_success() {
  local endpoint="$1"
  local timeout="$2"
  local method="${3:-GET}"
  local response
  response="$(curl -sS --max-time "${timeout}" -X "${method}" "${BASE_URL}${endpoint}")"
  python3 - "$endpoint" "$response" <<'PY'
import json,sys
endpoint=sys.argv[1]
raw=sys.argv[2]
try:
    payload=json.loads(raw)
except Exception:
    print(f"[FAIL] {endpoint}: non-JSON response")
    print(raw[:400])
    raise SystemExit(1)
if not payload.get("success", False):
    print(f"[FAIL] {endpoint}: success=false")
    print(raw[:400])
    raise SystemExit(1)
print(f"[OK] {endpoint}")
PY
}

echo "==> Refresh cache (timeout ${REFRESH_TIMEOUT}s)"
check_json_success "/api/cache/refresh" "${REFRESH_TIMEOUT}" "POST"

echo "==> Endpoint checks"
check_json_success "/api/process/connection" "${GET_TIMEOUT}"
check_json_success "/api/process/insights" "${GET_TIMEOUT}"
check_json_success "/api/process/context-coverage" "${GET_TIMEOUT}"
check_json_success "/api/process/celonis-context-layer" "${GET_TIMEOUT}"
check_json_success "/api/process/agents" "${GET_TIMEOUT}"
check_json_success "/api/exceptions/categories" "${GET_TIMEOUT}"
check_json_success "/api/process/runtime-tuning" "${GET_TIMEOUT}"

echo "==> Done"
