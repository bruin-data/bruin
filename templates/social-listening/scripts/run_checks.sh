#!/usr/bin/env bash
# Run every invariant query in checks/ and fail if any returns rows.
#   bash scripts/run_checks.sh [--env <environment>] [--bruin <path>]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRUIN=bruin
ENV_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV_ARGS=(--environment "$2"); shift 2 ;;
    --bruin) BRUIN="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

failed=0
for file in "$ROOT"/checks/*.sql; do
  name="$(basename "$file" .sql)"
  out="$("$BRUIN" query --connection social-listening-warehouse ${ENV_ARGS[@]+"${ENV_ARGS[@]}"} --output json --query "$(cat "$file")")"
  rows="$(printf '%s' "$out" | python3 -c 'import json,sys; t=sys.stdin.read(); print(len(json.loads(t[t.index("{"):]).get("rows") or []))')"
  if [[ "$rows" == "0" ]]; then
    echo "ok    $name"
  else
    echo "FAIL  $name ($rows violating rows)"
    printf '%s\n' "$out"
    failed=1
  fi
done
exit "$failed"
