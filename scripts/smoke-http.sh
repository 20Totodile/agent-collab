#!/usr/bin/env bash
# P0 smoke test: verify all HTTP API endpoints after server restart
set -euo pipefail

BASE="${AICOLLAB_BASE:-http://localhost:3009}"
TOKEN="${AICOLLAB_AUTH_TOKEN:-}"
if [ -z "$TOKEN" ]; then
  echo "AICOLLAB_AUTH_TOKEN env var required" >&2
  exit 1
fi
PASS=0
FAIL=0

check() {
  local name="$1" url="$2" expect="$3"
  local body
  body=$(curl -sf "$BASE$url" -H "Authorization: Bearer $TOKEN" 2>/dev/null) || { echo "FAIL $name (curl error)"; FAIL=$((FAIL+1)); return; }
  if echo "$body" | grep -q "$expect"; then
    echo "  OK $name"
    PASS=$((PASS+1))
  else
    echo "FAIL $name (missing: $expect)"
    FAIL=$((FAIL+1))
  fi
}

# AIC-129: assert an endpoint stays retired (HTTP 410 + matching body marker). Regression
# guard so `/api/terminal*` doesn't get accidentally re-mounted while we're in fork-mode.
check_status() {
  local name="$1" url="$2" method="$3" want_status="$4" body_expect="$5"
  local status body tmp
  tmp=$(mktemp)
  status=$(curl -s -o "$tmp" -w "%{http_code}" -X "$method" "$BASE$url" -H "Authorization: Bearer $TOKEN" 2>/dev/null || echo "000")
  body=$(cat "$tmp"); rm -f "$tmp"
  if [ "$status" = "$want_status" ] && echo "$body" | grep -q "$body_expect"; then
    echo "  OK $name (HTTP $status)"
    PASS=$((PASS+1))
  else
    echo "FAIL $name (got HTTP $status; want $want_status + body~'$body_expect')"
    FAIL=$((FAIL+1))
  fi
}

echo "=== ai-collab HTTP API smoke test ==="

check "group-roster"        "/group/roster"                      '"roster"'
check "search"              "/api/search?q=hello&limit=1"        '"results"'
check "transcript"          "/api/transcript?agent=agent1"       '"agent"'
check_status "terminal-retired"      "/api/terminal?session=agent1" GET  "410" "retired"
check_status "terminal-send-retired" "/api/terminal/send"           POST "410" "retired"
check "actor-styles"        "/api/actor-styles"                  '"styles"'
check "tasks"               "/tasks?status=draft"                '"tasks"'
check "workflow-templates"  "/tasks/workflow_templates"          '"templates"'

echo ""
echo "=== $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] || exit 1
