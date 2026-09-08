#!/usr/bin/env bash
#
# One entry point for running the payments pipeline locally.
#
#   ./demo.sh              # everything: start, replay 30 min, serve
#   ./demo.sh up 60        # same, with an hour of traffic
#   ./demo.sh replay 15    # just replay, no docker, no dashboard
#   ./demo.sh serve        # just the dashboard
#   ./demo.sh status       # what is running, and what is in the warehouse
#   ./demo.sh down         # stop and delete the containers
#
# The pipeline is scheduled every minute, so a demo means replaying a sequence of
# one-minute windows in order. They have to be consecutive and in chronological
# order: a restatement references the three windows immediately before its own,
# which is what exercises the lookback.
#
# A short block on the previous UTC day is replayed too, so both halves of
# serving_realtime_risk are populated -- the live day, and sealed history, where
# the non-additive KPIs are available.

set -euo pipefail

# Everything runs relative to this pipeline folder, so the demo works whatever
# the folder was named at `bruin init` time and from whatever directory it is
# called.
cd "$(dirname "$0")"

CFG="docker/bruin-local.yml"
PIPELINE="pipeline.yml"
COMPOSE="docker/compose.yml"
DEFAULT_MINUTES=30
HISTORY_MINUTES=10
SERVE_PORT="${SERVE_PORT:-8321}"

die() { printf '\nError: %s\n' "$1" >&2; exit 1; }
step() { printf '\n==> %s\n' "$1"; }

warehouse_reachable() {
  bruin query --config-file "$CFG" --connection clickhouse-default \
    --query "SELECT 1" >/dev/null 2>&1
}

require_tools() {
  local missing=()
  for t in docker bruin python3; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  [ ${#missing[@]} -eq 0 ] || die "missing required tool(s): ${missing[*]}"
  docker info >/dev/null 2>&1 || die "the Docker daemon is not running"
}

compose_up() {
  step "Starting PostgreSQL and ClickHouse"
  docker compose -f "$COMPOSE" up -d

  printf '    waiting for both to report healthy'
  local i state_pg state_ch
  for i in $(seq 1 60); do
    state_pg=$(docker inspect --format='{{.State.Health.Status}}' bruin-payments-postgres 2>/dev/null || echo starting)
    state_ch=$(docker inspect --format='{{.State.Health.Status}}' bruin-payments-clickhouse 2>/dev/null || echo starting)
    if [ "$state_pg" = healthy ] && [ "$state_ch" = healthy ]; then
      printf ' ok\n'; return 0
    fi
    printf '.'; sleep 2
  done
  printf '\n'
  die "containers did not become healthy (postgres=$state_pg clickhouse=$state_ch). Try: docker compose -f $COMPOSE logs"
}

# All windows are whole UTC minutes, derived from a single anchor captured once.
# Reading the clock per window would let the sequence drift forward as the replay
# runs, skipping minutes and breaking the restatement chain.
anchor_minus() {  # anchor_minus <minutes-before-anchor>
  python3 -c "
import datetime
t = datetime.datetime.fromtimestamp($ANCHOR_EPOCH, datetime.timezone.utc)
print((t - datetime.timedelta(minutes=$1)).strftime('%Y-%m-%d %H:%M'))
"
}

run_window() {  # run_window "<YYYY-MM-DD HH:MM>" [extra bruin flags]
  bruin run "$PIPELINE" \
    --config-file "$CFG" \
    --apply-interval-modifiers \
    ${2:-} \
    --start-date "$1:00" \
    --end-date "$1:59.999999" \
    >/dev/null
}

replay() {
  local minutes="$1"

  warehouse_reachable || die "cannot reach ClickHouse on 127.0.0.1:9000. Start it first: $0 up"

  ANCHOR_EPOCH=$(python3 -c "
import datetime
t = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
print(int(t.timestamp()))
")

  # Keep the live-day block inside today, so a run just after UTC midnight does
  # not quietly spill into yesterday.
  local since_midnight
  since_midnight=$(python3 -c "
import datetime
t = datetime.datetime.fromtimestamp($ANCHOR_EPOCH, datetime.timezone.utc)
print(t.hour * 60 + t.minute)
")
  if [ "$minutes" -gt "$since_midnight" ]; then
    minutes="$since_midnight"
    echo "    trimmed the live-day block to $minutes minute(s) to stay inside today (UTC)"
  fi
  [ "$minutes" -ge 1 ] || die "no full minute has elapsed today (UTC) yet; try again in a minute"

  step "Bootstrapping (full refresh, creates the tables)"
  # time_interval issues its delete before the table exists, so the very first
  # run of a fresh warehouse has to be a full refresh.
  run_window "$(anchor_minus $((24 * 60 + HISTORY_MINUTES)))" --full-refresh

  step "Replaying $HISTORY_MINUTES minute(s) on the previous UTC day (sealed history)"
  local i
  for i in $(seq $((24 * 60 + HISTORY_MINUTES - 1)) -1 $((24 * 60))); do
    run_window "$(anchor_minus "$i")"
  done

  step "Replaying $minutes minute(s) up to now (the live day)"
  for i in $(seq "$minutes" -1 1); do
    printf '\r    minute %s/%s' "$((minutes - i + 1))" "$minutes"
    run_window "$(anchor_minus "$i")"
  done
  printf '\n'
}

summary() {
  step "What the dashboard will show"
  bruin query --config-file "$CFG" --connection clickhouse-default --query "
SELECT
    if(is_today = 1, 'today (live, from the minute rollup)', 'earlier (sealed, from the daily KPI table)') AS source,
    min(txn_date)                       AS from_date,
    sum(txns)                           AS authorizations,
    round(sum(approved) / sum(txns), 4) AS approval_rate,
    sum(approved_volume)                AS approved_volume_usd,
    if(max(unique_cards) IS NULL, 'not additive - null by design', 'available') AS unique_cards
FROM bruin_payments.serving_realtime_risk
GROUP BY is_today
ORDER BY is_today"
}

serve() {
  step "Serving the dashboard"
  command -v dac >/dev/null 2>&1 || die "the 'dac' CLI is not installed; see https://github.com/bruin-data/dac"
  echo "    if port $SERVE_PORT is busy, dac moves to the next free one -- watch the URL it prints"
  exec dac serve --dir . --config "$CFG" --port "$SERVE_PORT" --open
}

status() {
  step "Containers"
  docker compose -f "$COMPOSE" ps
  step "Warehouse"
  if ! warehouse_reachable; then
    echo "    ClickHouse is not reachable -- the containers are not running. Start them: $0 up"
    return 0
  fi
  bruin query --config-file "$CFG" --connection clickhouse-default --query "
SELECT 'changelog versions' AS what, toString(count())            AS value FROM bruin_payments.raw_transaction_changes
UNION ALL SELECT 'distinct transactions', toString(uniqExact(transaction_id)) FROM bruin_payments.stg_transaction_changes
UNION ALL SELECT 'minutes in rollup',     toString(uniqExact(txn_minute))     FROM bruin_payments.rollup_txn_1m
UNION ALL SELECT 'latest minute',         toString(max(txn_minute))           FROM bruin_payments.rollup_txn_1m" 2>/dev/null \
    || echo "    tables not created yet -- run '$0 replay'"
}

down() {
  step "Stopping and deleting the containers"
  docker compose -f "$COMPOSE" down -v
}

case "${1:-up}" in
  up)
    require_tools
    compose_up
    replay "${2:-$DEFAULT_MINUTES}"
    summary
    serve
    ;;
  replay)
    require_tools
    replay "${2:-$DEFAULT_MINUTES}"
    summary
    ;;
  serve)  serve ;;
  status) status ;;
  down)   down ;;
  -h|--help|help)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    ;;
  *)
    die "unknown command '$1'. Try: up | replay | serve | status | down"
    ;;
esac
