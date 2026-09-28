#!/usr/bin/env bash
# Run one tier of the pipeline for a bounded window, for cron or CI schedulers.
#
#   bash scripts/run_tier.sh collect [minutes]   # raw collectors only; window = last N minutes (default 15)
#   bash scripts/run_tier.sh enrich              # staging -> assessments -> routing -> delivery -> queue, health
#   bash scripts/run_tier.sh report              # share of voice and health
#
# Extra arguments after the tier (or minutes) are passed to `bruin run`,
# e.g. --environment production --var demo_mode=false
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIER="${1:?usage: run_tier.sh collect|enrich|report [minutes] [bruin run args...]}"; shift

utc() { python3 -c "import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=int(sys.argv[1]))).strftime('%Y-%m-%d %H:%M:%S'))" "$1"; }

case "$TIER" in
  collect)
    MINUTES=15
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then MINUTES="$1"; shift; fi
    exec bruin run --tag collect --start-date "$(utc "$MINUTES")" --end-date "$(utc 0)" "$@" "$ROOT"
    ;;
  enrich|report)
    exec bruin run --tag "$TIER" --start-date "$(utc 1440)" --end-date "$(utc 0)" "$@" "$ROOT"
    ;;
  *) echo "unknown tier: $TIER" >&2; exit 2 ;;
esac
